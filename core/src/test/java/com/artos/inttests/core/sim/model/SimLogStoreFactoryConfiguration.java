package com.artos.inttests.core.sim.model;

import com.artos.api.core.server.conf.MemoryLogStoreFactoryConfiguration;
import com.artos.inttests.core.sim.perf.PerfRegistry;
import com.artos.spi.core.ILogStoreFactory;

public class SimLogStoreFactoryConfiguration extends MemoryLogStoreFactoryConfiguration {
    private final String endpoint;
    private final PerfRegistry perfRegistry;

    public SimLogStoreFactoryConfiguration(String endpoint, PerfRegistry perfRegistry, long minRetentionPeriod) {
        super(minRetentionPeriod);

        this.endpoint = endpoint;
        this.perfRegistry = perfRegistry;
    }

    @Override
    public ILogStoreFactory createFactory() {
        return new SimLogStoreFactory(endpoint, perfRegistry, getMinRetentionPeriod());
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof SimLogStoreFactoryConfiguration))
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        return 31 * getClass().hashCode();
    }
}
