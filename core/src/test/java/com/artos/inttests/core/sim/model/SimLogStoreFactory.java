package com.artos.inttests.core.sim.model;

import com.artos.impl.core.server.log.MemoryLogStore;
import com.artos.inttests.core.sim.perf.PerfRegistry;
import com.artos.spi.core.ILogStore;
import com.artos.spi.core.ILogStoreFactory;

public class SimLogStoreFactory implements ILogStoreFactory {
    private final String endpoint;
    private final PerfRegistry perfRegistry;
    private final long minRetentionPeriod;

    public SimLogStoreFactory(String endpoint, PerfRegistry perfRegistry, long minRetentionPeriod) {
        this.endpoint = endpoint;
        this.perfRegistry = perfRegistry;
        this.minRetentionPeriod = minRetentionPeriod;
    }

    @Override
    public ILogStore createLogStore() {
        return new SimLogStore(endpoint, new MemoryLogStore(minRetentionPeriod), perfRegistry);
    }
}
