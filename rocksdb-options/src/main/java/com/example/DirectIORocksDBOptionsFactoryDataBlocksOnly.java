package com.example;

public class DirectIORocksDBOptionsFactoryDataBlocksOnly extends BaseDirectIORocksDBOptionsFactory {

    @Override
    protected boolean enableBloomFilters() {
        return true;
    }

    @Override
    protected boolean enablePrefixFilters() {
        return true;
    }

    @Override
    protected boolean enableIndexOptimization() {
        return false;
    }

    @Override
    protected boolean cacheIndexAndFilterBlocksInBlockCache() {
        return false;
    }
}
