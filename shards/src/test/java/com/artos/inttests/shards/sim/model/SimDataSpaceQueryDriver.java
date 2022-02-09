package com.artos.inttests.shards.sim.model;

import com.artos.inttests.core.sim.model.SimQueryDriver;
import com.artos.inttests.core.sim.perf.PerfRegistry;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.ICompletionHandler;

public class SimDataSpaceQueryDriver extends SimQueryDriver {
    private ISimDataSpaceQueryChannel channel;

    public SimDataSpaceQueryDriver(String endpoint, long period, int batchSize, boolean enabled,
                                   PerfRegistry perfRegistry, SimDataSpacePublishDriver publishDriver) {
        super(endpoint, period, batchSize, enabled, perfRegistry, publishDriver);
    }

    public void setChannel(ISimDataSpaceQueryChannel channel) {
        this.channel = channel;
    }

    @Override
    protected void doSend(String message, ByteArray value, ICompletionHandler responseHandler) {
        channel.query(endpoint, value, responseHandler);
    }
}
