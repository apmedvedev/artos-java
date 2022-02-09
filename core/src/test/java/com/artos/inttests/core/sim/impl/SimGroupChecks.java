package com.artos.inttests.core.sim.impl;

import com.artos.inttests.core.sim.api.ISimGroupChecks;
import com.artos.inttests.core.sim.model.SimPublishDriver;
import com.artos.inttests.core.sim.model.SimServer;
import com.artos.inttests.core.sim.model.SimStateMachine;
import com.exametrika.common.l10n.DefaultMessage;
import com.exametrika.common.l10n.ILocalizedMessage;
import com.exametrika.common.l10n.Messages;
import com.exametrika.common.log.ILogger;
import com.exametrika.common.log.IMarker;
import com.exametrika.common.log.LogLevel;
import com.exametrika.common.log.Loggers;
import com.exametrika.common.utils.Threads;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class SimGroupChecks implements ISimGroupChecks {
    private static final IMessages messages = Messages.get(IMessages.class);
    private final ILogger logger = Loggers.get(getClass());
    private final IMarker marker = Loggers.getMarker("model");
    private final SimGroupDrivers drivers;
    private final List<SimGroupServers> serverGroups;

    public SimGroupChecks(SimGroupDrivers drivers, List<SimGroupServers> serverGroups) {
        this.drivers = drivers;
        this.serverGroups = serverGroups;
    }

    @Override
    public boolean checkConsistency(long stabilizationPeriod) {
        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.consistencyCheckStarted());

        drivers.enableDrivers(false);
        Threads.sleep(stabilizationPeriod);

        boolean result = true;
        Map<String, Long> driverState = new LinkedHashMap<>();
        Map<String, Long> combinedExpectedState = new LinkedHashMap<>();

        for (SimPublishDriver driver : drivers.getPublishDrivers()) {
            if (driver.getCounter() != 0)
                driverState.put(driver.getEndpoint(), driver.getCounter());
        }

        for (SimGroupServers serverGroup : serverGroups) {
            Map<String, Long> expectedState = null;
            for (SimServer server : serverGroup.getServers()) {
                SimStateMachine stateMachine = server.getStateMachine();
                Map<String, Long> serverState = stateMachine.getState();
                if (expectedState == null)
                    expectedState = serverState;
                else if (!serverState.equals(expectedState)) {
                    if (logger.isLogEnabled(LogLevel.ERROR))
                        logger.log(LogLevel.ERROR, marker, messages.consistencyCheckFailed(server.getEndpoint(),
                                new TreeMap(serverState), new TreeMap(expectedState)));

                    result = false;
                }
            }

            if (expectedState != null) {
                combinedExpectedState.putAll(expectedState);
            }
        }

        drivers.enableDrivers(true);

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.consistencyCheckCompleted(new TreeMap(driverState), new TreeMap(combinedExpectedState)));

        return result;
    }

    private interface IMessages {
        @DefaultMessage("Consistency check has been started.")
        ILocalizedMessage consistencyCheckStarted();

        @DefaultMessage("Consistency check is completed.\nDriver serverState: {0}\nExpected serverState: {1}")
        ILocalizedMessage consistencyCheckCompleted(TreeMap driverState, TreeMap expectedState);

        @DefaultMessage("Server ''{0}'' serverState differs from expected serverState.\nServer serverState: {1}\nExpected serverState: {2}")
        ILocalizedMessage consistencyCheckFailed(String endpoint, TreeMap serverState, TreeMap expectedState);
    }
}
