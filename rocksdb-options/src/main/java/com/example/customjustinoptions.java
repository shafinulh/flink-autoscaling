package com.example;

import org.apache.flink.contrib.streaming.state.RocksDBNativeMetricOptions;
import org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.Statistics;
import org.rocksdb.TableFormatConfig;
import org.rocksdb.WriteBufferManager;

import java.util.Collection;

public class customjustinoptions implements RocksDBOptionsFactory {

    private static final long BLOCK_CACHE_CAPACITY_BYTES = 610_125_013L;
    private static final int BLOCK_CACHE_NUM_SHARD_BITS = 6;
    private static final double BLOCK_CACHE_HIGH_PRIORITY_POOL_RATIO = 0.1d;

    private static final long WRITE_BUFFER_MANAGER_CAPACITY_BYTES = 110_450_005L;

    private static final long WRITE_BUFFER_SIZE_BYTES = 64L * 1024 * 1024;
    private static final int MAX_WRITE_BUFFER_NUMBER = 2;
    private static final int LEVEL0_FILE_NUM_COMPACTION_TRIGGER = 4;
    private static final int LEVEL0_SLOWDOWN_WRITES_TRIGGER = 20;
    private static final int LEVEL0_STOP_WRITES_TRIGGER = 36;
    private static final long TARGET_FILE_SIZE_BASE_BYTES = 64L * 1024 * 1024;
    private static final long MAX_BYTES_FOR_LEVEL_BASE_BYTES = 256L * 1024 * 1024;

    @Override
    public DBOptions createDBOptions(
            DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        Cache blockCache = new LRUCache(
                BLOCK_CACHE_CAPACITY_BYTES,
                BLOCK_CACHE_NUM_SHARD_BITS,
                false,
                BLOCK_CACHE_HIGH_PRIORITY_POOL_RATIO);
        handlesToClose.add(new PrimaryCacheHandle(blockCache));

        WriteBufferManager writeBufferManager =
                new WriteBufferManager(WRITE_BUFFER_MANAGER_CAPACITY_BYTES, blockCache);
        handlesToClose.add(writeBufferManager);

        Statistics statistics = new Statistics();
        handlesToClose.add(statistics);

        return currentOptions
                .setWriteBufferManager(writeBufferManager)
                .setUseDirectReads(false)
                .setUseDirectIoForFlushAndCompaction(false)
                .setDbLogDir("/data/rocksdb_native_logs")
                .setStatsDumpPeriodSec(0)
                .setNewTableReaderForCompactionInputs(true)
                .setCompactionReadaheadSize(2L * 1024 * 1024)
                .setMaxBackgroundJobs(2)
                .setMaxSubcompactions(1)
                .setStatistics(statistics);
    }

    @Override
    public ColumnFamilyOptions createColumnOptions(
            ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        Cache blockCache = handlesToClose.stream()
                .filter(PrimaryCacheHandle.class::isInstance)
                .map(PrimaryCacheHandle.class::cast)
                .map(PrimaryCacheHandle::cache)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Primary RocksDB block cache not found"));

        BlockBasedTableConfig tableConfig = resolveBlockBasedTableConfig(currentOptions);
        tableConfig
                .setCacheIndexAndFilterBlocks(true)
                .setCacheIndexAndFilterBlocksWithHighPriority(true)
                .setPinL0FilterAndIndexBlocksInCache(true)
                .setPinTopLevelIndexAndFilter(true)
                .setPartitionFilters(false)
                .setWholeKeyFiltering(true)
                .setBlockCache(blockCache);

        return currentOptions
                .setWriteBufferSize(WRITE_BUFFER_SIZE_BYTES)
                .setMaxWriteBufferNumber(MAX_WRITE_BUFFER_NUMBER)
                .setCompressionType(CompressionType.NO_COMPRESSION)
                .setNumLevels(7)
                .setLevel0FileNumCompactionTrigger(LEVEL0_FILE_NUM_COMPACTION_TRIGGER)
                .setLevel0SlowdownWritesTrigger(LEVEL0_SLOWDOWN_WRITES_TRIGGER)
                .setLevel0StopWritesTrigger(LEVEL0_STOP_WRITES_TRIGGER)
                .setTargetFileSizeBase(TARGET_FILE_SIZE_BASE_BYTES)
                .setMaxBytesForLevelBase(MAX_BYTES_FOR_LEVEL_BASE_BYTES)
                .setCompactionStyle(CompactionStyle.LEVEL)
                .setTableFormatConfig(tableConfig);
    }

    @Override
    public RocksDBNativeMetricOptions createNativeMetricsOptions(
            RocksDBNativeMetricOptions nativeMetricOptions) {
        return nativeMetricOptions;
    }

    private static BlockBasedTableConfig resolveBlockBasedTableConfig(
            ColumnFamilyOptions currentOptions) {
        TableFormatConfig config = currentOptions.tableFormatConfig();
        if (config == null) {
            return new BlockBasedTableConfig();
        }
        if (config instanceof BlockBasedTableConfig) {
            return (BlockBasedTableConfig) config;
        }
        throw new IllegalStateException(
                "Expected BlockBasedTableConfig, got " + config.getClass().getName());
    }

    private static final class PrimaryCacheHandle implements AutoCloseable {
        private final Cache cache;

        private PrimaryCacheHandle(Cache cache) {
            this.cache = cache;
        }

        private Cache cache() {
            return cache;
        }

        @Override
        public void close() {
            cache.close();
        }
    }
}
