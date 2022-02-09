package com.artos.inttests.core.sim.model;

import com.artos.api.core.server.IServerChannel;
import com.artos.api.core.server.StopType;
import com.artos.impl.core.server.impl.Server;
import com.artos.impl.core.server.impl.ServerRole;
import com.artos.impl.core.server.impl.State;
import com.exametrika.common.tests.Tests;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.Exceptions;
import com.exametrika.common.utils.ILifecycle;

public class SimServer implements ILifecycle {
    private final IServerChannel serverChannel;
    private final String endpoint;

    public SimServer(String endpoint, IServerChannel serverChannel) {
        this.endpoint = endpoint;
        Assert.notNull(serverChannel);

        this.serverChannel = serverChannel;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public ServerRole getRole() {
        try {
            Server server = Tests.get(serverChannel, "server");
            State state = Tests.get(server, "state");
            return state.getRole();
        } catch (Exception e) {
            return Exceptions.wrapAndThrow(e);
        }
    }

    public SimStateMachine getStateMachine() {
        return (SimStateMachine) serverChannel.getStateMachine();
    }

    @Override
    public void start() {
        serverChannel.start();
    }

    @Override
    public void stop() {
        serverChannel.stop();
    }

    public void stop(long timeout, StopType stopType) {
        serverChannel.stop(timeout, stopType);
    }
}
