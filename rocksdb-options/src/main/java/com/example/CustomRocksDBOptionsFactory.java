package com.example;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.contrib.streaming.state.RocksDBNativeMetricOptions;
import org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Set;

public class CustomRocksDBOptionsFactory implements RocksDBOptionsFactory {

    private static final Logger LOG =
        LoggerFactory.getLogger(CustomRocksDBOptionsFactory.class);

    // TODO: add mode where no WBM is used? RocksDB will allocate memory independently per ColumnFamily?
    private enum MemoryProvisioningMode {
        FLINK_MANAGED,
        FLINK_MANAGED_INDEP,
        MANUAL_INDEP,
        MANUAL_CHARGED
    }
    private static final MemoryProvisioningMode MEMORY_MODE = MemoryProvisioningMode.MANUAL_INDEP;

    // ----------------------------
    // Flink-managed memory inputs
    // ----------------------------
    private static final double WRITE_BUFFER_RATIO = 0.5;
    private static final double HIGH_PRIORITY_POOL_RATIO = 0.1;
    // numShardBits = -1 means it is automatically determined: every shard will be at least 512KB and number of shard bits will not exceed 6
    private static final int DEFAULT_BLOCK_CACHE_SHARD_BITS = 2;

    // fallback config values used when we cannot discover real TaskExecutor settings
    private static final long FALLBACK_TOTAL_FLINK_MEMORY_BYTES = (long) (1.55 * 1024 * 1024 * 1024L);
    private static final double FALLBACK_MANAGED_MEMORY_FRACTION = 0.4;
    private static final int FALLBACK_TASK_SLOTS_PER_TM = 4;
    private static final long FALLBACK_TOTAL_MANAGED_MEMORY_BYTES =
        (long) (FALLBACK_TOTAL_FLINK_MEMORY_BYTES * FALLBACK_MANAGED_MEMORY_FRACTION);

    // --------------------------
    // Manual memory provisioning
    // --------------------------
    private static final long MANUAL_BLOCK_CACHE_CAPACITY_BYTES = 130L * 1024 * 1024;
    private static final int MANUAL_BLOCK_CACHE_SHARD_BITS = 2;
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

    private static final boolean NEW_TABLE_READER_FOR_COMPACTION_INPUTS = false;
    private static final long COMPACTION_READAHEAD_SIZE_BYTES = 0L;

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
    private static final boolean ENABLE_STATS_DUMP = true;
    private static final int STATS_DUMP_PERIOD_SEC = 300;
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
            // Disable the new table reader path
            .setNewTableReaderForCompactionInputs(NEW_TABLE_READER_FOR_COMPACTION_INPUTS)
            .setCompactionReadaheadSize(COMPACTION_READAHEAD_SIZE_BYTES)

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

            tickers.add(TickerType.MEMTABLE_HIT);
            tickers.add(TickerType.MEMTABLE_MISS);

            tickers.add(TickerType.NUMBER_KEYS_READ);
            tickers.add(TickerType.NUMBER_KEYS_WRITTEN);

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

        BlockBasedTableConfig tableConfig = resolveBlockBasedTableConfig(currentOptions);
        tableConfig
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

    private static BlockBasedTableConfig resolveBlockBasedTableConfig(ColumnFamilyOptions currentOptions) {
        TableFormatConfig config = currentOptions.tableFormatConfig();
        if (config == null) {
            return new BlockBasedTableConfig();
        }
        if (config instanceof BlockBasedTableConfig) {
            return (BlockBasedTableConfig) config;
        }
        throw new IllegalStateException(
            "CustomRocksDBOptionsFactory requires BlockBasedTableConfig but found "
                + config.getClass().getName());
    }

    private static MemoryLayout resolveMemoryLayout() {
        switch (MEMORY_MODE) {
            case FLINK_MANAGED:
                return buildFlinkManagedLayout(true);
            case FLINK_MANAGED_INDEP:
                return buildFlinkManagedLayout(false);
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

    private static MemoryLayout buildFlinkManagedLayout(boolean chargeWriteBuffersToCache) {
        FlinkManagedMemoryStats stats = ManagedMemoryIntrospector.resolve();
        long perSlotManagedBytes = stats.perSlotManagedMemoryBytes();
        long blockCacheCapacity = calculateFlinkBlockCacheCapacity(perSlotManagedBytes);
        long writeBufferCapacity =
            calculateFlinkWriteBufferManagerCapacity(perSlotManagedBytes);
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                "Using Flink-managed memory: total={} bytes, slots={}, perSlot={} bytes, chargeWbm={}",
                stats.totalManagedMemoryBytes,
                stats.taskSlotsPerTm,
                perSlotManagedBytes,
                chargeWriteBuffersToCache);
        }
        return new MemoryLayout(
            blockCacheCapacity,
            DEFAULT_BLOCK_CACHE_SHARD_BITS,
            writeBufferCapacity,
            chargeWriteBuffersToCache // false means block cache + WBM operate independently
        );
    }

    private static void enableStatsDump(DBOptions options) {
        if (!ENABLE_STATS_DUMP) {
            return;
        }
        options.setStatsDumpPeriodSec(STATS_DUMP_PERIOD_SEC);
        // String flinkLogDirEnv = System.getenv("FLINK_LOG_DIR");
        // String baseLogDir = (flinkLogDirEnv != null && !flinkLogDirEnv.isEmpty()) ? flinkLogDirEnv : ".";
        // String rocksDbInstanceLogPath = baseLogDir + File.separator + ROCKSDB_LOG_SUBDIR_NAME;
        // File rocksDbLogDirFile = new File(rocksDbInstanceLogPath);
        // if (!rocksDbLogDirFile.exists()) {
        //     rocksDbLogDirFile.mkdirs();
        // }
        // options.setDbLogDir(rocksDbInstanceLogPath);
    }

    private static long calculateFlinkBlockCacheCapacity(long perSlotManagedBytes) {
        long sanitized = Math.max(perSlotManagedBytes, 1L);
        return (long) (((3 - WRITE_BUFFER_RATIO) * sanitized) / 3);
    }

    private static long calculateFlinkWriteBufferManagerCapacity(long perSlotManagedBytes) {
        long sanitized = Math.max(perSlotManagedBytes, 1L);
        return (long) ((2 * sanitized * WRITE_BUFFER_RATIO) / 3);
    }

    private static final class FlinkManagedMemoryStats {
        private final long totalManagedMemoryBytes;
        private final int taskSlotsPerTm;

        private FlinkManagedMemoryStats(long totalManagedMemoryBytes, int taskSlotsPerTm) {
            this.totalManagedMemoryBytes = totalManagedMemoryBytes;
            this.taskSlotsPerTm = Math.max(taskSlotsPerTm, 1);
        }

        private long perSlotManagedMemoryBytes() {
            return Math.max(totalManagedMemoryBytes / taskSlotsPerTm, 1L);
        }
    }

    private static final class ManagedMemoryIntrospector {
        private static final Object LOCK = new Object();
        private static volatile FlinkManagedMemoryStats cachedStats;

        private static FlinkManagedMemoryStats resolve() {
            FlinkManagedMemoryStats stats = cachedStats;
            if (stats != null) {
                return stats;
            }
            synchronized (LOCK) {
                stats = cachedStats;
                if (stats != null) {
                    return stats;
                }
                cachedStats = detect();
                return cachedStats;
            }
        }

        private static FlinkManagedMemoryStats detect() {
            try {
                Configuration configuration = loadFlinkConfiguration();
                TaskExecutorProcessSpec spec =
                    TaskExecutorProcessUtils.processSpecFromConfig(configuration);
                long totalManaged = spec.getManagedMemorySize().getBytes();
                int slots = spec.getNumSlots();
                if (totalManaged <= 0 || slots <= 0) {
                    LOG.warn(
                        "Invalid managed memory detection (total={}, slots={}), falling back to defaults.",
                        totalManaged,
                        slots);
                    return fallbackStats();
                }
                LOG.info(
                    "Detected managed memory from Flink configuration: total={} bytes, slots={}",
                    totalManaged,
                    slots);
                return new FlinkManagedMemoryStats(totalManaged, slots);
            } catch (Throwable t) {
                LOG.warn(
                    "Unable to determine Flink managed memory from configuration, falling back to defaults.",
                    t);
                return fallbackStats();
            }
        }

        private static Configuration loadFlinkConfiguration() {
            String confDir = System.getenv("FLINK_CONF_DIR");
            if (confDir != null && !confDir.isEmpty()) {
                return GlobalConfiguration.loadConfiguration(confDir);
            }
            return GlobalConfiguration.loadConfiguration();
        }

        private static FlinkManagedMemoryStats fallbackStats() {
            return new FlinkManagedMemoryStats(
                FALLBACK_TOTAL_MANAGED_MEMORY_BYTES, FALLBACK_TASK_SLOTS_PER_TM);
        }
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
