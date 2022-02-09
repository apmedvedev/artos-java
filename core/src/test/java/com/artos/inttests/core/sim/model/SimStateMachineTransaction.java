package com.artos.inttests.core.sim.model;

import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.spi.core.IStateMachineTransaction;
import com.artos.spi.core.ServerState;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.ICompletionHandler;
import com.exametrika.common.utils.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class SimStateMachineTransaction implements IStateMachineTransaction {
    private final boolean readonly;
    private ServerState serverState;
    private GroupConfiguration configuration;
    private final SimStateMachine stateMachine;
    private final List<Pair<Long, ByteArray>> publishState = new ArrayList<>();
    private final List<ByteArray> queryState = new ArrayList<>();
    private final List<Pair<UUID, Pair<ByteArray, ICompletionHandler<ByteArray>>>> subscriptions = new ArrayList<>();
    private final List<UUID> unsubscriptions = new ArrayList<>();

    public SimStateMachineTransaction(boolean readonly, ServerState serverState, GroupConfiguration configuration,
                                      SimStateMachine stateMachine) {
        this.readonly = readonly;
        this.serverState = serverState;
        this.configuration = configuration;
        this.stateMachine = stateMachine;
    }

    @Override
    public boolean isReadOnly() {
        return readonly;
    }

    @Override
    public GroupConfiguration readConfiguration() {
        return configuration;
    }

    @Override
    public void writeConfiguration(GroupConfiguration configuration) {
        Assert.notNull(configuration);
        Assert.checkState(!readonly);

        this.configuration = configuration;
    }

    @Override
    public ServerState readState() {
        return serverState;
    }

    @Override
    public void writeState(ServerState serverState) {
        Assert.notNull(serverState);
        Assert.checkState(!readonly);

        this.serverState = serverState;
    }

    @Override
    public void publish(long logIndex, ByteArray data) {
        Assert.notNull(data);
        Assert.checkState(!readonly);

        if (!stateMachine.getDriver().isDummy())
            publishState.add(new Pair<>(logIndex, data));
    }

    @Override
    public void query(ByteArray request, ICompletionHandler<ByteArray> responseHandler) {
        if (!stateMachine.getDriver().isDummy())
            queryState.add(request);

        responseHandler.onSucceeded(request);
    }

    @Override
    public void subscribe(UUID subscriptionId, ByteArray request, ICompletionHandler<ByteArray> responseHandler) {
        subscriptions.add(new Pair<>(subscriptionId, new Pair<>(request, responseHandler)));
    }

    @Override
    public void unsubscribe(UUID subscriptionId) {
        unsubscriptions.add(subscriptionId);
    }

    @Override
    public void commit() {
        stateMachine.commit(serverState, configuration, publishState, queryState, subscriptions, unsubscriptions);
    }
}
