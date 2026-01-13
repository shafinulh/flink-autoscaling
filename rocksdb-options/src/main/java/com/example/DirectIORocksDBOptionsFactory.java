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

import java.util.Collection;
import java.lang.reflect.Field;
import java.util.Set;

public class DirectIORocksDBOptionsFactory implements RocksDBOptionsFactory {

    private static final boolean USE_DIRECT_READS = true;
    private static final boolean USE_DIRECT_IO_FOR_FLUSH_AND_COMPACTION = true;
    private static final boolean ENABLE_BLOOM_FILTERS = true;
    private static final boolean ENABLE_PREFIX_AND_BLOOM_FILTERS = true;
    private static final boolean ENABLE_TICKERS = true;
    // Set to 0 to disable stats dumps
    private static final int STATS_DUMP_PERIOD_SEC = 300;
    
    private static final int BLOOM_FILTER_BITS_PER_KEY = 10;
    private static final boolean BLOOM_FILTER_BLOCK_BASED_MODE = false;
    private static final int FIXED_PREFIX_BYTES = 22;
    private static final boolean CACHE_INDEX_AND_FILTER_BLOCKS = true;
    private static final boolean CACHE_INDEX_AND_FILTER_BLOCKS_WITH_HIGH_PRIORITY = true;
    private static final boolean PIN_L0_FILTER_AND_INDEX_BLOCKS = true;
    private static final boolean PIN_TOP_LEVEL_INDEX_AND_FILTER = true;
    private static final boolean USE_PARTITIONED_INDEX_FILTERS = true;

    @Override
    public DBOptions createDBOptions(DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        enableStatsDump(currentOptions);
        return currentOptions
            .setUseDirectReads(USE_DIRECT_READS)
            .setUseDirectIoForFlushAndCompaction(USE_DIRECT_IO_FOR_FLUSH_AND_COMPACTION);
    }

    @Override
    public ColumnFamilyOptions createColumnOptions(
            ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        if (!ENABLE_BLOOM_FILTERS && !ENABLE_PREFIX_AND_BLOOM_FILTERS) {
            return currentOptions;
        }

        BlockBasedTableConfig tableConfig = resolveBlockBasedTableConfig(currentOptions);
        BloomFilter bloomFilter = new BloomFilter(
            BLOOM_FILTER_BITS_PER_KEY,
            BLOOM_FILTER_BLOCK_BASED_MODE);
        handlesToClose.add(bloomFilter);
        tableConfig
            .setCacheIndexAndFilterBlocks(CACHE_INDEX_AND_FILTER_BLOCKS)
            .setCacheIndexAndFilterBlocksWithHighPriority(CACHE_INDEX_AND_FILTER_BLOCKS_WITH_HIGH_PRIORITY)
            .setPinL0FilterAndIndexBlocksInCache(PIN_L0_FILTER_AND_INDEX_BLOCKS)
            .setPinTopLevelIndexAndFilter(PIN_TOP_LEVEL_INDEX_AND_FILTER)
            .setIndexType(USE_PARTITIONED_INDEX_FILTERS
                ? IndexType.kTwoLevelIndexSearch
                : IndexType.kBinarySearch)
            .setPartitionFilters(USE_PARTITIONED_INDEX_FILTERS)
            .setFilterPolicy(bloomFilter)
            .setWholeKeyFiltering(!ENABLE_PREFIX_AND_BLOOM_FILTERS);

        ColumnFamilyOptions configured = currentOptions.setTableFormatConfig(tableConfig);
        if (ENABLE_PREFIX_AND_BLOOM_FILTERS) {
            applyFixedPrefixExtractorIfConfigured(configured);
        }
        return configured;
    }

    @Override
    public ReadOptions createReadOptions(ReadOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        if (!ENABLE_PREFIX_AND_BLOOM_FILTERS) {
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
            "DirectIORocksDBOptionsFactory requires BlockBasedTableConfig but found "
                + config.getClass().getName());
    }

    private static void enableStatsDump(DBOptions options) {
        if (STATS_DUMP_PERIOD_SEC <= 0) {
            options.setStatsDumpPeriodSec(0);
            return;
        }
        options.setStatsDumpPeriodSec(STATS_DUMP_PERIOD_SEC);
    }

    private static void applyFixedPrefixExtractorIfConfigured(ColumnFamilyOptions options) {
        if (FIXED_PREFIX_BYTES <= 0) {
            return;
        }
        options.useFixedLengthPrefixExtractor(FIXED_PREFIX_BYTES);
        options.setOptimizeFiltersForHits(true);
    }
}
