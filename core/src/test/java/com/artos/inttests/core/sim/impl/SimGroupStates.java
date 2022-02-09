package com.artos.inttests.core.sim.impl;

import com.artos.inttests.core.sim.model.ISimStateListener;
import com.exametrika.common.l10n.DefaultMessage;
import com.exametrika.common.l10n.ILocalizedMessage;
import com.exametrika.common.l10n.Messages;
import com.exametrika.common.log.ILogger;
import com.exametrika.common.log.IMarker;
import com.exametrika.common.log.LogLevel;
import com.exametrika.common.log.Loggers;

public class SimGroupStates {
    private static final IMessages messages = Messages.get(IMessages.class);
    private final ILogger logger = Loggers.get(getClass());
    private final IMarker marker = Loggers.getMarker("model");
    private String requestedState;
    private String requestedStateEndpoint;
    private boolean stateFired;
    private final Object stateCondition = new Object();

    public void waitForState(String state) {
        waitForState(state, null);
    }

    public void waitForState(String state, String endpoint) {
        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.beginWaitForState(state, endpoint));

        synchronized (stateCondition) {
            requestedState = state;
            requestedStateEndpoint = endpoint;
            stateFired = false;
            while (!stateFired) {
                try {
                    stateCondition.wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.endWaitForState(state, endpoint));
    }

    private class GroupSimStateListener implements ISimStateListener {
        @Override
        public void onState(String endpoint, String state) {
            synchronized (stateCondition) {
                if (requestedState != null && requestedState.equals(state)) {
                    if (requestedStateEndpoint == null || requestedStateEndpoint.equals(endpoint)) {
                        stateFired = true;
                        stateCondition.notify();
                    }
                }
            }
        }
    }

    private interface IMessages {
        @DefaultMessage("Begin waiting for serverState. State: {0}, endpoint: {1}.")
        ILocalizedMessage beginWaitForState(String state, String endpoint);

        @DefaultMessage("End waiting for serverState. State: {0}, endpoint: {1}.")
        ILocalizedMessage endWaitForState(String state, String endpoint);
    }
}
