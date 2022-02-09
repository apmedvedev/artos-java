package com.artos.tests.core.mocks;

import com.artos.api.core.server.conf.ServerConfiguration;
import com.artos.impl.core.message.ServerRequest;
import com.artos.impl.core.server.impl.IServer;
import com.artos.impl.core.server.impl.IServerPeer;
import com.exametrika.common.utils.Pair;

import java.util.ArrayList;
import java.util.List;

public class ServerMock implements IServer {
    private List<Pair<IServerPeer, ServerRequest>> requests = new ArrayList<>();

    public List<Pair<IServerPeer, ServerRequest>> getRequests() {
        return requests;
    }

    @Override
    public IServerPeer createPeer(ServerConfiguration configuration) {
        return null;
    }

    @Override
    public void send(IServerPeer peer, ServerRequest request) {
        requests.add(new Pair<>(peer, request));
    }
}
