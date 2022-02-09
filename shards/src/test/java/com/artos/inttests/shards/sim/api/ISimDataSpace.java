package com.artos.inttests.shards.sim.api;

import com.artos.inttests.core.sim.api.ISimGroupChecks;
import com.artos.inttests.core.sim.api.ISimGroupDrivers;
import com.artos.inttests.core.sim.api.ISimGroupLinks;
import com.artos.inttests.core.sim.api.ISimGroupServers;
import com.exametrika.common.utils.ILifecycle;

import java.io.File;
import java.util.List;

public interface ISimDataSpace extends ILifecycle {
    ISimGroupLinks getLinks();

    ISimGroupDrivers getDrivers();

    ISimDataSpaceClients getClients();

    List<ISimGroupServers> getServerGroups();

    ISimGroupChecks getChecks();

    void writeStatistics(File directory);
}
