package com.example;

public class DirectIORocksDBOptionsFactoryIndexOnly extends BaseDirectIORocksDBOptionsFactory {

    @Override
    protected boolean enableBloomFilters() {
        return false;
    }

    @Override
    protected boolean enablePrefixFilters() {
        return false;
    }

    @Override
    protected boolean enableIndexOptimization() {
        return true;
    }
}
