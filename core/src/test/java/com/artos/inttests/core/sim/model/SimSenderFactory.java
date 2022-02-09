package com.artos.inttests.core.sim.model;

import com.artos.inttests.core.sim.perf.PerfRegistry;
import com.artos.spi.core.IMessageSender;
import com.artos.spi.core.IMessageSenderFactory;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.ICompletionHandler;

public class SimSenderFactory implements IMessageSenderFactory {
    private final SimLink link;
    private final PerfRegistry perfRegistry;

    public SimSenderFactory(SimLink link, PerfRegistry perfRegistry) {
        this.link = link;
        this.perfRegistry = perfRegistry;
    }

    @Override
    public IMessageSender createSender(String endpoint, ICompletionHandler<ByteArray> responseHandler) {
        return new SimSender(link, endpoint, perfRegistry, responseHandler);
    }
}
