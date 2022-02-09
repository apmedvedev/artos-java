package com.artos.impl.core.client;

import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.impl.core.message.PublishRequest;
import com.exametrika.common.utils.ILifecycle;

public interface IClientProtocol extends ILifecycle {
    boolean isConnected();

    boolean isRequestInProgress();

    GroupConfiguration getGroupConfiguration();

    IPublishClientProtocol getPublishProtocol();

    IQueryClientProtocol getQueryProtocol();

    void sendRequest(PublishRequest request);

    void onLeaderDisconnected();
}
