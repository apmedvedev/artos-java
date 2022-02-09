package com.artos.api.core.server.conf;

import com.artos.impl.core.server.log.MemoryLogStoreFactory;
import com.artos.spi.core.ILogStoreFactory;
import com.artos.spi.core.conf.LogStoreFactoryConfiguration;
import com.exametrika.common.utils.Objects;

public class MemoryLogStoreFactoryConfiguration extends LogStoreFactoryConfiguration {
    private final long minRetentionPeriod;

    public MemoryLogStoreFactoryConfiguration(long minRetentionPeriod) {
        this.minRetentionPeriod = minRetentionPeriod;
    }

    public long getMinRetentionPeriod() {
        return minRetentionPeriod;
    }

    @Override
    public ILogStoreFactory createFactory() {
        return new MemoryLogStoreFactory(minRetentionPeriod);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof MemoryLogStoreFactoryConfiguration))
            return false;

        MemoryLogStoreFactoryConfiguration configuration = (MemoryLogStoreFactoryConfiguration) o;
        return minRetentionPeriod == configuration.minRetentionPeriod;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(minRetentionPeriod);
    }
}
