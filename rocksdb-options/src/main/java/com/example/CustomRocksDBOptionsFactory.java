package com.example;

import org.apache.flink.contrib.streaming.state.RocksDBNativeMetricOptions;
import org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory;
import org.rocksdb.*;

import java.io.File;
import java.util.Collection;
import java.util.Set;
import java.lang.reflect.Field;

public class CustomRocksDBOptionsFactory implements RocksDBOptionsFactory {

    // ----------------------------------------
    // MEMORY / CACHE CONFIGURATION CONSTANTS
    // ----------------------------------------
    // Ratio of cache space reserved for high-priority blocks (index & filter)
    private static final double HIGH_PRIORITY_POOL_RATIO = 0.1;

    // Size of the data block cache (in bytes)
    private static final long BLOCK_CACHE_SIZE = 260L * 1024 * 1024;

    // Number of bits to shard the block cache
    private static final int BLOCK_CACHE_SHARD_BITS = 2;

    // Per-Column-Family write-buffer size (memtable) in bytes
    private static final long WRITE_BUFFER_SIZE = 64L * 1024 * 1024;
    // Total write-buffer limit across all column families (for WriteBufferManager)
    private static final long TOTAL_WRITE_BUFFER_LIMIT = 256L * 1024 * 1024;
    // Number of memtables per column family before forcing flush
    private static final int MAX_WRITE_BUFFER_NUMBER = 2; // irrelevant since i see all memtables flushed as soon as they are inactive

    // Number of L0 files needed before triggering a compaction
    private static final int L0_FILE_NUM_COMPACTION_TRIGGER = 4;
    // Target size (bytes) for L1 SST files
    private static final long TARGET_FILE_SIZE_BASE = 64L * 1024 * 1024;
    // Max bytes for the base level (L0) before compaction is triggered
    private static final long MAX_BYTES_FOR_LEVEL_BASE = 256L * 1024 * 1024;

    // ----------------------------------------
    // COMPACTION / FLUSH / I/O CONFIGURATION
    // ----------------------------------------
    // Background threads
    // private static final int MAX_BACKGROUND_FLUSHES     = 1;
    // private static final int MAX_BACKGROUND_COMPACTIONS = 6;
    private static final int MAX_BACKGROUND_JOBS        = 2;

    // Subcompactions per compaction task
    private static final int MAX_SUBCOMPACTIONS = 1;

    // stat dumps to see histogram types - turn off when not neededAdd commentMore actions
    // private static final int STATS_DUMP_PERIOD_SEC = 90; // Dump stats every 90 seconds
    // private static final String ROCKSDB_LOG_SUBDIR_NAME = "rocksdb_native_logs"; // Subdirectory for these logs

    @Override
    public DBOptions createDBOptions(DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        Cache blockCache = new LRUCache(
            BLOCK_CACHE_SIZE,
            BLOCK_CACHE_SHARD_BITS,
            false,
            HIGH_PRIORITY_POOL_RATIO
        );
        handlesToClose.add(blockCache);

        Cache writeBufferDummyCache = new LRUCache(1);
        handlesToClose.add(writeBufferDummyCache);

        WriteBufferManager writeBufferManager = new WriteBufferManager(
            TOTAL_WRITE_BUFFER_LIMIT,
            writeBufferDummyCache // do not charge write buffer against the main block cache
        );
        handlesToClose.add(writeBufferManager);

        Statistics statistics = new Statistics();
        statistics.setStatsLevel(StatsLevel.ALL);
        handlesToClose.add(statistics);

        // currentOptions.setStatsDumpPeriodSec(STATS_DUMP_PERIOD_SEC);
        // String flinkLogDirEnv = System.getenv("FLINK_LOG_DIR");
        // String baseLogDir = (flinkLogDirEnv != null && !flinkLogDirEnv.isEmpty()) ? flinkLogDirEnv : ".";
        // String rocksDbInstanceLogPath = baseLogDir + File.separator + ROCKSDB_LOG_SUBDIR_NAME;
        // File rocksDbLogDirFile = new File(rocksDbInstanceLogPath);
        // if (!rocksDbLogDirFile.exists()) {
        //     rocksDbLogDirFile.mkdirs();
        // }
        // currentOptions.setDbLogDir(rocksDbInstanceLogPath);
        return currentOptions
            // Use the WriteBufferManager instead of letting each CF allocate independently
            .setWriteBufferManager(writeBufferManager)
            // Enable direct reads so cache misses don't go through OS page cache → we measure real disk I/O
            .setUseDirectReads(true)
            // Leave direct writes disabled (default) so memtable flushes go through page cache
            .setUseDirectIoForFlushAndCompaction(false)

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
            .filter(h -> h instanceof Cache)
            .map(h -> (Cache) h)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Block cache not found in handlesToClose"));

        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig()
            .setCacheIndexAndFilterBlocks(true)
            .setCacheIndexAndFilterBlocksWithHighPriority(true)
            .setPinL0FilterAndIndexBlocksInCache(true)
            .setPinTopLevelIndexAndFilter(true)
            .setBlockCache(blockCache);

        return currentOptions
            // Write Path Config
            .setWriteBufferSize(WRITE_BUFFER_SIZE)
            .setMaxWriteBufferNumber(MAX_WRITE_BUFFER_NUMBER)
            .setTargetFileSizeBase(TARGET_FILE_SIZE_BASE)

            // Compaction Trigger
            .setLevel0FileNumCompactionTrigger(L0_FILE_NUM_COMPACTION_TRIGGER)
            .setMaxBytesForLevelBase(MAX_BYTES_FOR_LEVEL_BASE)

            // To pin index/filter, but evict data blocks
            .setTableFormatConfig(tableConfig);
    }
}
