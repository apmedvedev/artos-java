package com.artos.inttests.core.sim.model;

import com.artos.inttests.core.sim.perf.PerfRegistry;
import com.artos.spi.core.IMessageReceiver;
import com.artos.spi.core.IMessageListener;
import com.artos.spi.core.IMessageListenerFactory;

public class SimListenerFactory implements IMessageListenerFactory {
    private final SimLink link;
    private final PerfRegistry perfRegistry;

    public SimListenerFactory(SimLink link, PerfRegistry perfRegistry) {
        this.link = link;
        this.perfRegistry = perfRegistry;
    }

    @Override
    public IMessageListener createListener(IMessageReceiver messageReceiver) {
        return new SimListener(link, perfRegistry, messageReceiver);
    }
}
