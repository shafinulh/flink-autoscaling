package com.example;

import org.apache.flink.contrib.streaming.state.RocksDBNativeMetricOptions;
import org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory;
import org.rocksdb.*;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Set;

public class CustomRocksDBOptionsFactory implements RocksDBOptionsFactory {

    // TODO: add mode where no WBM is used? RocksDB will allocate memory independently per ColumnFamily?
    private enum MemoryProvisioningMode {
        FLINK_MANAGED,
        MANUAL_INDEP,
        MANUAL_CHARGED
    }
    private static final MemoryProvisioningMode MEMORY_MODE = MemoryProvisioningMode.FLINK_MANAGED;

    // ----------------------------
    // Flink-managed memory inputs
    // ----------------------------
    // total Flink memory scraped from TM logs (usually ~75‑85% of total process memory)
    private static final long TOTAL_FLINK_MEMORY_BYTES = (long) (3.35 * 1024 * 1024 * 1024L);
    // flink memory config settings (flink-conf.yaml)
    private static final double MANAGED_MEMORY_FRACTION = 0.4;
    private static final int TASK_SLOTS_PER_TM = 4;
    private static final double WRITE_BUFFER_RATIO = 0.5;
    private static final double HIGH_PRIORITY_POOL_RATIO = 0.1;


    // default Flink memory allocations (from RocksDBMemoryControllerUtils)
    private static final long TOTAL_MANAGED_MEMORY_BYTES =
        (long) (TOTAL_FLINK_MEMORY_BYTES * MANAGED_MEMORY_FRACTION);
    private static final long PER_SLOT_MANAGED_MEMORY_BYTES =
        TOTAL_MANAGED_MEMORY_BYTES / TASK_SLOTS_PER_TM;

    private static final long FLINK_BLOCK_CACHE_CAPACITY_BYTES =
        (long) (((3 - WRITE_BUFFER_RATIO) * PER_SLOT_MANAGED_MEMORY_BYTES) / 3);
    // numShardBits = -1 means it is automatically determined: every shard will be at least 512KB and number of shard bits will not exceed 6
    private static final int DEFAULT_BLOCK_CACHE_SHARD_BITS = -1;

    private static final long FLINK_WRITE_BUFFER_MANAGER_CAPACITY_BYTES =
        (long) ((2 * PER_SLOT_MANAGED_MEMORY_BYTES * WRITE_BUFFER_RATIO) / 3);

    // --------------------------
    // Manual memory provisioning
    // --------------------------
    private static final long MANUAL_BLOCK_CACHE_CAPACITY_BYTES = 132L * 1024 * 1024;
    private static final int MANUAL_BLOCK_CACHE_SHARD_BITS = 6;
    private static final long MANUAL_WRITE_BUFFER_MANAGER_CAPACITY_BYTES = 53L * 1024 * 1024;

    // --------------------------
    // Default RocksDB settings
    // --------------------------
    // using Page Cache
    // in default Flink direct reads/writes not used. in Justin it is
    private static final boolean USE_DIRECT_READS = true;
    private static final boolean USE_DIRECT_IO_FOR_FLUSH_AND_COMPACTION = true;
    
    // write path and compaction settings
    private static final long WRITE_BUFFER_SIZE = 64L * 1024 * 1024; // memtable
    private static final int MAX_WRITE_BUFFER_NUMBER = 2; // num memtable before forcing flush
    private static final int L0_FILE_NUM_COMPACTION_TRIGGER = 4; 
    private static final long TARGET_FILE_SIZE_BASE = 64L * 1024 * 1024; // L1 SST file size
    private static final long MAX_BYTES_FOR_LEVEL_BASE = 256L * 1024 * 1024; // max L0 total bytes before 

    // threads for compaction and background jobs
    private static final int MAX_BACKGROUND_JOBS = 2;
    private static final int MAX_SUBCOMPACTIONS = 1;

    // read path block cache settings
    private static final boolean CACHE_INDEX_AND_FILTER_BLOCKS = true;
    private static final boolean CACHE_INDEX_AND_FILTER_BLOCKS_WITH_HIGH_PRIORITY = true;
    private static final boolean PIN_L0_FILTER_AND_INDEX_BLOCKS = true;
    private static final boolean PIN_TOP_LEVEL_INDEX_AND_FILTER = true;
    private static final boolean USE_PARTITIONED_INDEX_FILTERS = false;

    // stat dumps to see histogram types
    private static final boolean ENABLE_STATS_DUMP = false;
    private static final int STATS_DUMP_PERIOD_SEC = 90;
    private static final String ROCKSDB_LOG_SUBDIR_NAME = "rocksdb_native_logs";

    @Override
    public DBOptions createDBOptions(DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        MemoryLayout layout = resolveMemoryLayout();

        Cache blockCache = new LRUCache(
            layout.blockCacheCapacityBytes,
            layout.blockCacheShardBits,
            false,
            HIGH_PRIORITY_POOL_RATIO
        );
        handlesToClose.add(new CacheHandle(blockCache, true));

        Cache writeBufferChargeCache;
        if (layout.chargeWriteBuffersToCache) {
            writeBufferChargeCache = blockCache;
        } else {
            writeBufferChargeCache = new LRUCache(1);
            handlesToClose.add(new CacheHandle(writeBufferChargeCache, false));
        }

        WriteBufferManager writeBufferManager = new WriteBufferManager(
            layout.writeBufferManagerCapacityBytes,
            writeBufferChargeCache
        );
        handlesToClose.add(writeBufferManager);

        Statistics statistics = new Statistics();
        statistics.setStatsLevel(StatsLevel.ALL);
        handlesToClose.add(statistics);

        enableStatsDump(currentOptions);
        return currentOptions
            // Use the WriteBufferManager instead of letting each CF allocate independently
            .setWriteBufferManager(writeBufferManager)
            // Enable direct reads so cache misses don't go through OS page cache → we measure real disk I/O
            .setUseDirectReads(USE_DIRECT_READS)
            // Enable direct IO for flush/compaction so RocksDB bypasses the OS page cache on write path
            .setUseDirectIoForFlushAndCompaction(USE_DIRECT_IO_FOR_FLUSH_AND_COMPACTION)

            // RocksDB background threads
            .setMaxBackgroundJobs(MAX_BACKGROUND_JOBS)
            .setMaxSubcompactions(MAX_SUBCOMPACTIONS)

            // Attach statistics for Prometheus
            .setStatistics(statistics);
    }

    @Override
    public RocksDBNativeMetricOptions createNativeMetricsOptions(RocksDBNativeMetricOptions nativeMetricOptions) {
        try {
            Field f = RocksDBNativeMetricOptions.class.getDeclaredField("monitorTickerTypes");
            f.setAccessible(true);
            @SuppressWarnings("unchecked")
            Set<TickerType> tickers = (Set<TickerType>) f.get(nativeMetricOptions);

            // Add all block-cache related TickerTypes so Prometheus sees index/filter/data hit/miss
            tickers.add(TickerType.BLOCK_CACHE_ADD);
            tickers.add(TickerType.BLOCK_CACHE_ADD_FAILURES);
            tickers.add(TickerType.BLOCK_CACHE_INDEX_HIT);
            tickers.add(TickerType.BLOCK_CACHE_INDEX_MISS);
            tickers.add(TickerType.BLOCK_CACHE_FILTER_HIT);
            tickers.add(TickerType.BLOCK_CACHE_FILTER_MISS);
            tickers.add(TickerType.BLOCK_CACHE_DATA_HIT);
            tickers.add(TickerType.BLOCK_CACHE_DATA_MISS);
            tickers.add(TickerType.BLOCK_CACHE_BYTES_READ);
            tickers.add(TickerType.BLOCK_CACHE_BYTES_WRITE);

            // Also watch compaction key-drop metrics for visibility
            tickers.add(TickerType.COMPACTION_KEY_DROP_OBSOLETE);
            tickers.add(TickerType.COMPACTION_KEY_DROP_USER);

            return nativeMetricOptions;
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Failed to register block cache metrics", e);
        }
    }

    @Override
    public ColumnFamilyOptions createColumnOptions(
            ColumnFamilyOptions currentOptions,
            Collection<AutoCloseable> handlesToClose) {

        Cache blockCache = handlesToClose.stream()
            .filter(CacheHandle.class::isInstance)
            .map(CacheHandle.class::cast)
            .filter(CacheHandle::isPrimary)
            .map(CacheHandle::cache)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Block cache not found in handlesToClose"));

        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig()
            .setCacheIndexAndFilterBlocks(CACHE_INDEX_AND_FILTER_BLOCKS)
            .setCacheIndexAndFilterBlocksWithHighPriority(CACHE_INDEX_AND_FILTER_BLOCKS_WITH_HIGH_PRIORITY)
            .setPinL0FilterAndIndexBlocksInCache(PIN_L0_FILTER_AND_INDEX_BLOCKS)
            .setPinTopLevelIndexAndFilter(PIN_TOP_LEVEL_INDEX_AND_FILTER)
            .setPartitionFilters(USE_PARTITIONED_INDEX_FILTERS)
            .setBlockCache(blockCache);

        return currentOptions
            // Write Path Config
            .setWriteBufferSize(WRITE_BUFFER_SIZE)
            .setMaxWriteBufferNumber(MAX_WRITE_BUFFER_NUMBER)
            .setTargetFileSizeBase(TARGET_FILE_SIZE_BASE)

            // Compaction Config
            .setLevel0FileNumCompactionTrigger(L0_FILE_NUM_COMPACTION_TRIGGER)
            .setMaxBytesForLevelBase(MAX_BYTES_FOR_LEVEL_BASE)

            // Table Format Config
            .setTableFormatConfig(tableConfig);
    }

    private static MemoryLayout resolveMemoryLayout() {
        switch (MEMORY_MODE) {
            case FLINK_MANAGED:
                return new MemoryLayout(
                    FLINK_BLOCK_CACHE_CAPACITY_BYTES,
                    DEFAULT_BLOCK_CACHE_SHARD_BITS,
                    FLINK_WRITE_BUFFER_MANAGER_CAPACITY_BYTES,
                    true // WBM charges against the block cache (Flink default)
                );
            case MANUAL_INDEP:
                return new MemoryLayout(
                    MANUAL_BLOCK_CACHE_CAPACITY_BYTES,
                    MANUAL_BLOCK_CACHE_SHARD_BITS,
                    MANUAL_WRITE_BUFFER_MANAGER_CAPACITY_BYTES,
                    false // manual mode size definition and keep block cache + WBM fully independent
                );
            case MANUAL_CHARGED:
                return new MemoryLayout(
                    MANUAL_BLOCK_CACHE_CAPACITY_BYTES,
                    MANUAL_BLOCK_CACHE_SHARD_BITS,
                    MANUAL_WRITE_BUFFER_MANAGER_CAPACITY_BYTES,
                    true // manual mode but WBM charges against the block cache
                );
        }
        throw new IllegalStateException();
    }

    private static void enableStatsDump(DBOptions options) {
        if (!ENABLE_STATS_DUMP) {
            return;
        }
        options.setStatsDumpPeriodSec(STATS_DUMP_PERIOD_SEC);
        String flinkLogDirEnv = System.getenv("FLINK_LOG_DIR");
        String baseLogDir = (flinkLogDirEnv != null && !flinkLogDirEnv.isEmpty()) ? flinkLogDirEnv : ".";
        String rocksDbInstanceLogPath = baseLogDir + File.separator + ROCKSDB_LOG_SUBDIR_NAME;
        File rocksDbLogDirFile = new File(rocksDbInstanceLogPath);
        if (!rocksDbLogDirFile.exists()) {
            rocksDbLogDirFile.mkdirs();
        }
        options.setDbLogDir(rocksDbInstanceLogPath);
    }

    private static final class MemoryLayout {
        private final long blockCacheCapacityBytes;
        private final int blockCacheShardBits;
        private final long writeBufferManagerCapacityBytes;
        private final boolean chargeWriteBuffersToCache;

        private MemoryLayout(
                long blockCacheCapacityBytes,
                int blockCacheShardBits,
                long writeBufferManagerCapacityBytes,
                boolean chargeWriteBuffersToCache) {
            this.blockCacheCapacityBytes = blockCacheCapacityBytes;
            this.blockCacheShardBits = blockCacheShardBits;
            this.writeBufferManagerCapacityBytes = writeBufferManagerCapacityBytes;
            this.chargeWriteBuffersToCache = chargeWriteBuffersToCache;
        }
    }

    private static final class CacheHandle implements AutoCloseable {
        private final Cache cache;
        private final boolean primary;

        private CacheHandle(Cache cache, boolean primary) {
            this.cache = cache;
            this.primary = primary;
        }

        private Cache cache() {
            return cache;
        }

        private boolean isPrimary() {
            return primary;
        }

        @Override
        public void close() {
            cache.close();
        }
    }
}
