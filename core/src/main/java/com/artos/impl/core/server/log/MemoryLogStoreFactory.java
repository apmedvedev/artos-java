package com.artos.impl.core.server.log;

import com.artos.spi.core.ILogStore;
import com.artos.spi.core.ILogStoreFactory;

public class MemoryLogStoreFactory implements ILogStoreFactory {
    private final long minRetentionPeriod;

    public MemoryLogStoreFactory(long minRetentionPeriod) {
        this.minRetentionPeriod = minRetentionPeriod;
    }

    @Override
    public ILogStore createLogStore() {
        return new MemoryLogStore(minRetentionPeriod);
    }
}
