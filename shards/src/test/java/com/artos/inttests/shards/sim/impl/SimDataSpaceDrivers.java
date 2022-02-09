package com.artos.inttests.shards.sim.impl;

import com.artos.inttests.core.sim.impl.SimGroupDrivers;
import com.artos.inttests.core.sim.model.SimPublishDriver;
import com.artos.inttests.core.sim.model.SimQueryDriver;
import com.artos.inttests.core.sim.perf.PerfRegistry;
import com.artos.inttests.shards.sim.model.SimDataSpacePublishDriver;
import com.artos.inttests.shards.sim.model.SimDataSpaceQueryDriver;

public class SimDataSpaceDrivers extends SimGroupDrivers {
    public SimDataSpaceDrivers(long publishDriverPeriod, int publishDriverBatchSize, long queryDriverPeriod, int queryDriverBatchSize, PerfRegistry perfRegistry, boolean dummy) {
        super(publishDriverPeriod, publishDriverBatchSize, queryDriverPeriod, queryDriverBatchSize, perfRegistry, dummy);
    }

    @Override
    protected SimPublishDriver createPublishDriver(String endpoint, boolean client) {
        return new SimDataSpacePublishDriver(endpoint, publishDriverPeriod, publishDriverBatchSize, false, perfRegistry, client, dummy);
    }

    @Override
    protected SimQueryDriver createQueryDriver(String endpoint, SimPublishDriver publishDriver) {
        return new SimDataSpaceQueryDriver(endpoint, queryDriverPeriod, queryDriverBatchSize, false, perfRegistry,
            (SimDataSpacePublishDriver) publishDriver);
    }

    @Override
    protected boolean canBeEnabled(SimQueryDriver driver) {
        return driver.isClient();
    }

    @Override
    protected boolean canBeEnabled(SimPublishDriver driver) {
        return driver.isClient();
    }
}
