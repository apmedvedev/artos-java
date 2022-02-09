package com.artos.inttests.core.sim.model;

import com.artos.inttests.core.sim.perf.PerfRegistry;
import com.artos.spi.core.IGroupChannel;
import com.artos.spi.core.IStateMachine;
import com.artos.spi.core.IStateMachineFactory;

import java.util.UUID;

public class SimStateMachineFactory implements IStateMachineFactory {
    private final UUID serverId;
    private final String endpoint;
    private final PerfRegistry perfRegistry;
    private final SimPublishDriver driver;
    private final boolean enableStateMachineChecks;
    private final long subscriptionEventPeriod;
    private final int subscriptionEventBatchSize;
    private final SimStateMachine stateMachine;

    public SimStateMachineFactory(UUID serverId, String endpoint, PerfRegistry perfRegistry, SimPublishDriver driver,
                                  boolean enableStateMachineChecks, long subscriptionEventPeriod, int subscriptionEventBatchSize) {
        this.serverId = serverId;
        this.endpoint = endpoint;
        this.perfRegistry = perfRegistry;
        this.driver = driver;
        this.enableStateMachineChecks = enableStateMachineChecks;
        this.subscriptionEventPeriod = subscriptionEventPeriod;
        this.subscriptionEventBatchSize = subscriptionEventBatchSize;
        this.stateMachine = null;
    }

    public SimStateMachineFactory(SimStateMachine stateMachine) {
        this.stateMachine = stateMachine;
        this.serverId = null;
        this.endpoint = null;
        this.perfRegistry = null;
        this.driver = null;
        this.enableStateMachineChecks = false;
        this.subscriptionEventPeriod = 0;
        this.subscriptionEventBatchSize = 0;
    }

    @Override
    public IStateMachine createStateMachine(IGroupChannel groupChannel) {
        if (stateMachine != null)
            return stateMachine;
        else
            return new SimStateMachine(serverId, endpoint, perfRegistry, driver, groupChannel, enableStateMachineChecks,
                subscriptionEventPeriod, subscriptionEventBatchSize);
    }
}
