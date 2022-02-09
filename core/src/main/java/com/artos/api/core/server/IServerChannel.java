package com.artos.api.core.server;

import com.artos.api.core.server.conf.ServerChannelConfiguration;
import com.artos.spi.core.IStateMachine;
import com.exametrika.common.utils.ILifecycle;

public interface IServerChannel extends IMembershipService, ILifecycle {
    ServerChannelConfiguration getConfiguration();

    boolean isStarted();

    IStateMachine getStateMachine();

    void addChannelListener(IServerChannelListener listener);

    void removeChannelListener(IServerChannelListener listener);

    void removeAllChannelListeners();

    void stop(long timeout, StopType stopType);
}
