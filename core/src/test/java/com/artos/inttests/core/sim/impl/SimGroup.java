package com.artos.inttests.core.sim.impl;

import com.artos.inttests.core.sim.api.ISimGroup;
import com.artos.inttests.core.sim.perf.PerfRegistry;
import com.exametrika.common.l10n.DefaultMessage;
import com.exametrika.common.l10n.ILocalizedMessage;
import com.exametrika.common.l10n.Messages;
import com.exametrika.common.log.ILogger;
import com.exametrika.common.log.IMarker;
import com.exametrika.common.log.LogLevel;
import com.exametrika.common.log.Loggers;
import com.exametrika.common.utils.Files;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class SimGroup implements ISimGroup {
    private static final int CLIENT_COUNT = 10;
    private static final int SERVER_COUNT = 5;
    private static final long PUBLISH_DRIVER_PERIOD = 1;
    private static final int PUBLISH_DRIVER_BATCH_SIZE = 1;
    private static final long QUERY_DRIVER_PERIOD = 1;
    private static final int QUERY_DRIVER_BATCH_SIZE = 1;
    private static final long SUBSCRIPTION_EVENT_PERIOD = 1;
    private static final int SUBSCRIPTION_EVENT_BATCH_SIZE = 1;
    private static final boolean MEASUREMENTS_ENABLED = true;
    private static final boolean DUMMY_MESSAGES = false;
    private static final boolean SINGLE_SERVER = false;

    private static final IMessages messages = Messages.get(IMessages.class);
    private final ILogger logger = Loggers.get(getClass());
    private final IMarker marker = Loggers.getMarker("model");
    private final PerfRegistry perfRegistry = new PerfRegistry(Arrays.asList(0, 1, 5, 10), MEASUREMENTS_ENABLED);
    private final SimGroupLinks links;
    private final SimGroupDrivers drivers;
    private final SimGroupClients clients;
    private final SimGroupServers servers;
    private final SimGroupChecks checks;
    private final SimGroupStates states;

    public SimGroup() {
        links = new SimGroupLinks(perfRegistry);
        drivers = new SimGroupDrivers(PUBLISH_DRIVER_PERIOD, PUBLISH_DRIVER_BATCH_SIZE,
            QUERY_DRIVER_PERIOD, QUERY_DRIVER_BATCH_SIZE, perfRegistry, DUMMY_MESSAGES);
        servers = new SimGroupServers("test", SERVER_COUNT, perfRegistry, links, drivers, true,
            SUBSCRIPTION_EVENT_PERIOD, SUBSCRIPTION_EVENT_BATCH_SIZE, SINGLE_SERVER);
        clients = new SimGroupClients(CLIENT_COUNT, perfRegistry, drivers, servers, links);
        checks = new SimGroupChecks(drivers, Collections.singletonList(servers));
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
    public SimGroupClients getClients() {
        return clients;
    }

    @Override
    public SimGroupServers getServers() {
        return servers;
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
            logger.log(LogLevel.INFO, marker, messages.groupStarting());

        servers.start();
        clients.start();

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.groupStarted());
    }

    @Override
    public synchronized void stop() {
        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.groupStopping());

        drivers.stop();
        clients.stop();
        servers.stop();

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.groupStopped());
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
        @DefaultMessage("Group is starting...")
        ILocalizedMessage groupStarting();

        @DefaultMessage("Group has been started...")
        ILocalizedMessage groupStarted();

        @DefaultMessage("Group is stopping...")
        ILocalizedMessage groupStopping();

        @DefaultMessage("Group has been stopped...")
        ILocalizedMessage groupStopped();

        @DefaultMessage("Performance statistics:\n{0}")
        ILocalizedMessage performanceStatistics(String statistics);

        @DefaultMessage("Statistics has been written to: {0}")
        ILocalizedMessage statisticsWritten(File directory);
    }
}
