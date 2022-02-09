package com.artos.impl.core.server.impl;

import com.artos.api.core.server.conf.ServerConfiguration;
import com.artos.impl.core.message.ServerRequest;

public interface IServer {
    IServerPeer createPeer(ServerConfiguration configuration);

    void send(IServerPeer peer, ServerRequest request);
}
