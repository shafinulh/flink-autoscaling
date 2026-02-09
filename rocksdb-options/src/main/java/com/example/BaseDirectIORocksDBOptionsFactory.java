package com.example;

import org.apache.flink.contrib.streaming.state.RocksDBNativeMetricOptions;
import org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.IndexType;
import org.rocksdb.ReadOptions;
import org.rocksdb.TableFormatConfig;
import org.rocksdb.TickerType;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Set;

public abstract class BaseDirectIORocksDBOptionsFactory implements RocksDBOptionsFactory {

    private static final boolean USE_DIRECT_READS = true;
    private static final boolean USE_DIRECT_IO_FOR_FLUSH_AND_COMPACTION = true;
    private static final boolean ENABLE_TICKERS = true;
    // Set to 0 to disable stats dumps
    private static final int STATS_DUMP_PERIOD_SEC = 600;
    private static final String ROCKSDB_LOG_DIR = "/data/rocksdb_native_logs";

    private static final int BLOOM_FILTER_BITS_PER_KEY = 10;
    private static final boolean BLOOM_FILTER_BLOCK_BASED_MODE = false;
    private static final int FIXED_PREFIX_BYTES = 22;
    private static final boolean CACHE_INDEX_AND_FILTER_BLOCKS = true;
    private static final boolean CACHE_INDEX_AND_FILTER_BLOCKS_WITH_HIGH_PRIORITY = true;
    private static final boolean PIN_L0_FILTER_AND_INDEX_BLOCKS = true;
    private static final boolean PIN_TOP_LEVEL_INDEX_AND_FILTER = true;

    protected abstract boolean enableBloomFilters();

    protected abstract boolean enablePrefixFilters();

    protected abstract boolean enableIndexOptimization();

    protected boolean cacheIndexAndFilterBlocksInBlockCache() {
        return CACHE_INDEX_AND_FILTER_BLOCKS;
    }

    @Override
    public DBOptions createDBOptions(DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        configureDbLogDir(currentOptions);
        enableStatsDump(currentOptions);
        return currentOptions
            .setUseDirectReads(USE_DIRECT_READS)
            .setUseDirectIoForFlushAndCompaction(USE_DIRECT_IO_FOR_FLUSH_AND_COMPACTION);
    }

    @Override
    public ColumnFamilyOptions createColumnOptions(
            ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        boolean usePrefix = enablePrefixFilters();
        boolean useBloom = enableBloomFilters() || usePrefix;
        boolean useIndex = enableIndexOptimization();
        boolean cacheIndexAndFilterBlocks = cacheIndexAndFilterBlocksInBlockCache();

        if (!useBloom && !useIndex) {
            return currentOptions;
        }

        BlockBasedTableConfig tableConfig = resolveBlockBasedTableConfig(currentOptions);
        tableConfig
            .setCacheIndexAndFilterBlocks(cacheIndexAndFilterBlocks)
            .setCacheIndexAndFilterBlocksWithHighPriority(
                cacheIndexAndFilterBlocks && CACHE_INDEX_AND_FILTER_BLOCKS_WITH_HIGH_PRIORITY)
            .setPinL0FilterAndIndexBlocksInCache(
                cacheIndexAndFilterBlocks && PIN_L0_FILTER_AND_INDEX_BLOCKS)
            .setPinTopLevelIndexAndFilter(
                cacheIndexAndFilterBlocks && PIN_TOP_LEVEL_INDEX_AND_FILTER)
            .setIndexType(useIndex ? IndexType.kTwoLevelIndexSearch : IndexType.kBinarySearch)
            .setPartitionFilters(useIndex);

        if (useBloom) {
            BloomFilter bloomFilter = new BloomFilter(
                BLOOM_FILTER_BITS_PER_KEY,
                BLOOM_FILTER_BLOCK_BASED_MODE);
            handlesToClose.add(bloomFilter);
            tableConfig
                .setFilterPolicy(bloomFilter)
                .setWholeKeyFiltering(!usePrefix);
        }

        ColumnFamilyOptions configured = currentOptions.setTableFormatConfig(tableConfig);
        if (usePrefix) {
            applyFixedPrefixExtractorIfConfigured(configured);
        }
        return configured;
    }

    @Override
    public ReadOptions createReadOptions(ReadOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        if (!enablePrefixFilters()) {
            return currentOptions;
        }
        return currentOptions
            .setPrefixSameAsStart(true)
            .setTotalOrderSeek(false);
    }

    @Override
    public RocksDBNativeMetricOptions createNativeMetricsOptions(
            RocksDBNativeMetricOptions nativeMetricOptions) {
        if (!ENABLE_TICKERS) {
            return nativeMetricOptions;
        }
        try {
            Field f = RocksDBNativeMetricOptions.class.getDeclaredField("monitorTickerTypes");
            f.setAccessible(true);
            @SuppressWarnings("unchecked")
            Set<TickerType> tickers = (Set<TickerType>) f.get(nativeMetricOptions);

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

    private static BlockBasedTableConfig resolveBlockBasedTableConfig(ColumnFamilyOptions currentOptions) {
        TableFormatConfig config = currentOptions.tableFormatConfig();
        if (config == null) {
            return new BlockBasedTableConfig();
        }
        if (config instanceof BlockBasedTableConfig) {
            return (BlockBasedTableConfig) config;
        }
        throw new IllegalStateException(
            "BaseDirectIORocksDBOptionsFactory requires BlockBasedTableConfig but found "
                + config.getClass().getName());
    }

    private static void enableStatsDump(DBOptions options) {
        if (STATS_DUMP_PERIOD_SEC <= 0) {
            options.setStatsDumpPeriodSec(0);
            return;
        }
        options.setStatsDumpPeriodSec(STATS_DUMP_PERIOD_SEC);
    }

    private static void configureDbLogDir(DBOptions options) {
        File dir = new File(ROCKSDB_LOG_DIR);
        if (!dir.exists() && !dir.mkdirs()) {
            return;
        }
        options.setDbLogDir(ROCKSDB_LOG_DIR);
    }

    private static void applyFixedPrefixExtractorIfConfigured(ColumnFamilyOptions options) {
        if (FIXED_PREFIX_BYTES <= 0) {
            return;
        }
        options.useFixedLengthPrefixExtractor(FIXED_PREFIX_BYTES);
        options.setOptimizeFiltersForHits(true);
    }
}
