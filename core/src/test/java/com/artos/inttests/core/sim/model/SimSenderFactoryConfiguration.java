package com.artos.inttests.core.sim.model;

import com.artos.inttests.core.sim.perf.PerfRegistry;
import com.artos.spi.core.IMessageSenderFactory;
import com.artos.spi.core.conf.MessageSenderFactoryConfiguration;

public class SimSenderFactoryConfiguration extends MessageSenderFactoryConfiguration {
    private final SimLink link;
    private final PerfRegistry perfRegistry;

    public SimSenderFactoryConfiguration(SimLink link, PerfRegistry perfRegistry) {
        this.link = link;
        this.perfRegistry = perfRegistry;
    }

    @Override
    public IMessageSenderFactory createFactory() {
        return new SimSenderFactory(link, perfRegistry);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof SimSenderFactoryConfiguration))
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        return 31 * getClass().hashCode();
    }
}
