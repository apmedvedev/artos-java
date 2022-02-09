package com.artos.inttests.shards.sim.impl;

import com.artos.inttests.core.sim.api.ISimGroupServers;
import com.artos.inttests.core.sim.impl.SimGroupChecks;
import com.artos.inttests.core.sim.impl.SimGroupDrivers;
import com.artos.inttests.core.sim.impl.SimGroupLinks;
import com.artos.inttests.core.sim.impl.SimGroupServers;
import com.artos.inttests.core.sim.impl.SimGroupStates;
import com.artos.inttests.core.sim.perf.PerfRegistry;
import com.artos.inttests.shards.sim.api.ISimDataSpace;
import com.exametrika.common.l10n.DefaultMessage;
import com.exametrika.common.l10n.ILocalizedMessage;
import com.exametrika.common.l10n.Messages;
import com.exametrika.common.log.ILogger;
import com.exametrika.common.log.IMarker;
import com.exametrika.common.log.LogLevel;
import com.exametrika.common.log.Loggers;
import com.exametrika.common.utils.Files;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SimDataSpace implements ISimDataSpace {
    private static final int GROUP_COUNT = 5;
    private static final int CLIENT_COUNT = 10;
    private static final int SERVERS_PER_GROUP_COUNT = 5;
    private static final long PUBLISH_DRIVER_PERIOD = 1;
    private static final int PUBLISH_DRIVER_BATCH_SIZE = 1;
    private static final long QUERY_DRIVER_PERIOD = 10000;
    private static final int QUERY_DRIVER_BATCH_SIZE = 1;
    private static final long SUBSCRIPTION_EVENT_PERIOD = 10000;
    private static final int SUBSCRIPTION_EVENT_BATCH_SIZE = 1;
    private static final boolean MEASUREMENTS_ENABLED = true;
    private static final boolean DUMMY_MESSAGES = false;
    private static final boolean SINGLE_SERVER = false;

    private static final IMessages messages = Messages.get(IMessages.class);
    private final ILogger logger = Loggers.get(getClass());
    private final IMarker marker = Loggers.getMarker("model");
    private final PerfRegistry perfRegistry = new PerfRegistry(Arrays.asList(0, 1, 5, 10), MEASUREMENTS_ENABLED);
    private final SimGroupLinks links;
    private final SimDataSpaceDrivers drivers;
    private final SimDataSpaceClients clients;
    private final List<SimGroupServers> serverGroups = new ArrayList<>();
    private final SimGroupChecks checks;
    private final SimGroupStates states;

    public SimDataSpace() {
        links = new SimGroupLinks(perfRegistry);
        drivers = new SimDataSpaceDrivers(PUBLISH_DRIVER_PERIOD, PUBLISH_DRIVER_BATCH_SIZE,
            QUERY_DRIVER_PERIOD, QUERY_DRIVER_BATCH_SIZE, perfRegistry, DUMMY_MESSAGES);
        for (int i = 1; i <= GROUP_COUNT; i++)
            serverGroups.add(new SimGroupServers("group" + i, SERVERS_PER_GROUP_COUNT, perfRegistry, links, drivers, true,
                SUBSCRIPTION_EVENT_PERIOD, SUBSCRIPTION_EVENT_BATCH_SIZE, SINGLE_SERVER));
        clients = new SimDataSpaceClients(CLIENT_COUNT, perfRegistry, drivers, serverGroups, links);
        checks = new SimGroupChecks(drivers, serverGroups);
        states = new SimGroupStates();
    }

    @Override
    public SimGroupLinks getLinks() {
        return links;
    }

    @Override
    public SimGroupDrivers getDrivers() {
        return drivers;
    }

    @Override
    public SimDataSpaceClients getClients() {
        return clients;
    }

    @Override
    public List<ISimGroupServers> getServerGroups() {
        return (List)serverGroups;
    }

    @Override
    public SimGroupChecks getChecks() {
        return checks;
    }

    public SimGroupStates getStates() {
        return states;
    }

    @Override
    public synchronized void start() {
        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.dataSpaceStarting());

        for (SimGroupServers serverGroup : serverGroups)
            serverGroup.start();

        clients.start();

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.dataSpaceStarted());
    }

    @Override
    public synchronized void stop() {
        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.dataSpaceStopping());

        drivers.stop();
        clients.stop();

        for (SimGroupServers serverGroup : serverGroups)
            serverGroup.stop();

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.dataSpaceStopped());
    }

    @Override
    public synchronized void writeStatistics(File directory) {
        directory.mkdirs();
        Files.emptyDir(directory);
        for (Map.Entry<String, Object> entry : perfRegistry.getStatistics().entrySet()) {
            File file = new File(directory, entry.getKey() + ".json");
            Files.write(file, entry.getValue().toString());
        }

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.statisticsWritten(directory));
    }

    private interface IMessages {
        @DefaultMessage("Data space is starting...")
        ILocalizedMessage dataSpaceStarting();

        @DefaultMessage("Data space has been started...")
        ILocalizedMessage dataSpaceStarted();

        @DefaultMessage("Data space is stopping...")
        ILocalizedMessage dataSpaceStopping();

        @DefaultMessage("Data space has been stopped...")
        ILocalizedMessage dataSpaceStopped();

        @DefaultMessage("Performance statistics:\n{0}")
        ILocalizedMessage performanceStatistics(String statistics);

        @DefaultMessage("Statistics has been written to: {0}")
        ILocalizedMessage statisticsWritten(File directory);
    }
}
