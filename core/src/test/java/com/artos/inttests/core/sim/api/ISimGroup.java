package com.artos.inttests.core.sim.api;

import com.exametrika.common.utils.ILifecycle;

import java.io.File;

public interface ISimGroup extends ILifecycle {
    ISimGroupLinks getLinks();

    ISimGroupDrivers getDrivers();

    ISimGroupClients getClients();

    ISimGroupServers getServers();

    ISimGroupChecks getChecks();

    void writeStatistics(File directory);
}
