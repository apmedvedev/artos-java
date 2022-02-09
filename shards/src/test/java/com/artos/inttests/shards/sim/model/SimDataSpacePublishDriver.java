package com.artos.inttests.shards.sim.model;

import com.artos.inttests.core.sim.model.SimPublishDriver;
import com.artos.inttests.core.sim.perf.PerfRegistry;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.ICompletionHandler;

public class SimDataSpacePublishDriver extends SimPublishDriver {
    private ISimDataSpacePublishChannel channel;

    public SimDataSpacePublishDriver(String endpoint, long period, int batchSize, boolean enabled,
                                     PerfRegistry perfRegistry, boolean client, boolean dummy) {
        super(endpoint, period, batchSize, enabled, perfRegistry, client, dummy);
    }

    public void setChannel(ISimDataSpacePublishChannel channel) {
        this.channel = channel;
    }

    @Override
    protected void doSend(String message, ByteArray value, ICompletionHandler commitHandler) {
        channel.publish(endpoint, value, commitHandler);
    }
}
