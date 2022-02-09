package com.artos.api.core.server;

import com.artos.api.core.server.conf.ServerChannelConfiguration;
import com.artos.spi.core.IStateMachineFactory;

public interface IServerChannelFactory {
    IServerChannel createChannel(ServerChannelConfiguration configuration, IStateMachineFactory stateMachineFactory);
}
