package com.artos.inttests.core.sim.model;

import com.artos.api.core.client.IClientChannel;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.ICompletionHandler;
import com.exametrika.common.utils.ILifecycle;

import java.util.UUID;

public class SimClient implements ISimPublishChannel, ISimQueryChannel, ILifecycle {
    private final IClientChannel clientChannel;
    private final String endpoint;
    private SimClientStore clientStore;

    public SimClient(String endpoint, IClientChannel clientChannel) {
        Assert.notNull(clientChannel);

        this.endpoint = endpoint;
        this.clientChannel = clientChannel;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public SimClientStore getClientStore() {
        return clientStore;
    }

    public void setClientStore(SimClientStore clientStore) {
        Assert.notNull(clientStore);

        this.clientStore = clientStore;
    }

    @Override
    public void start() {
        clientChannel.start();
    }

    @Override
    public void stop() {
        clientChannel.stop();
    }

    @Override
    public void publish(ByteArray value, ICompletionHandler<ByteArray> commitHandler) {
        clientChannel.publish(value, commitHandler);
    }

    @Override
    public void query(ByteArray request, ICompletionHandler<ByteArray> responseHandler) {
        clientChannel.query(request, responseHandler);
    }

    @Override
    public void subscribe(UUID subscriptionId, ByteArray request, ICompletionHandler<ByteArray> responseHandler) {
        clientChannel.subscribe(subscriptionId, request, responseHandler);
    }

    @Override
    public void unsubscribe(UUID subscriptionId) {
        clientChannel.unsubscribe(subscriptionId);
    }
}
