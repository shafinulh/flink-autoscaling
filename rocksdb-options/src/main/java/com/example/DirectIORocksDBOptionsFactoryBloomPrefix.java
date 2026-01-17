package com.example;

public class DirectIORocksDBOptionsFactoryBloomPrefix extends BaseDirectIORocksDBOptionsFactory {

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
}
