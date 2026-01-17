package com.example;

public class DirectIORocksDBOptionsFactoryIndexBloom extends BaseDirectIORocksDBOptionsFactory {

    @Override
    protected boolean enableBloomFilters() {
        return true;
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
