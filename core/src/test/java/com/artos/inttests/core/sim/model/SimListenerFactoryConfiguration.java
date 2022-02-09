package com.artos.inttests.core.sim.model;

import com.artos.inttests.core.sim.perf.PerfRegistry;
import com.artos.spi.core.IMessageListenerFactory;
import com.artos.spi.core.conf.MessageListenerFactoryConfiguration;

public class SimListenerFactoryConfiguration extends MessageListenerFactoryConfiguration {
    private final SimLink link;
    private final PerfRegistry perfRegistry;

    public SimListenerFactoryConfiguration(SimLink link, PerfRegistry perfRegistry) {
        this.link = link;
        this.perfRegistry = perfRegistry;
    }

    @Override
    public IMessageListenerFactory createFactory() {
        return new SimListenerFactory(link, perfRegistry);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof SimListenerFactoryConfiguration))
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        return 31 * getClass().hashCode();
    }
}
