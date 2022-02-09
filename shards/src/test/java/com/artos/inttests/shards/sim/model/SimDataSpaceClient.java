package com.artos.inttests.shards.sim.model;

import com.artos.api.shards.client.IDataSpaceClientChannel;
import com.artos.inttests.core.sim.model.SimClientStore;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.ICompletionHandler;
import com.exametrika.common.utils.ILifecycle;

public class SimDataSpaceClient<K> implements ISimDataSpacePublishChannel<K>, ISimDataSpaceQueryChannel<K>, ILifecycle {
    private final IDataSpaceClientChannel clientChannel;
    private final String endpoint;
    private SimClientStore clientStore;

    public SimDataSpaceClient(String endpoint, IDataSpaceClientChannel clientChannel) {
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
    public void publish(K key, ByteArray value, ICompletionHandler<ByteArray> commitHandler) {
        clientChannel.publish(key, value, commitHandler);
    }

    @Override
    public void query(K key, ByteArray request, ICompletionHandler<ByteArray> responseHandler) {
        clientChannel.query(key, request, responseHandler);
    }
}
