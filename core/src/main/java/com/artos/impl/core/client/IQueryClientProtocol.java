package com.artos.impl.core.client;

import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.api.core.server.conf.ServerConfiguration;
import com.artos.spi.core.IMessageSender;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.ICompletionHandler;
import com.exametrika.common.utils.ILifecycle;

import java.util.UUID;

public interface IQueryClientProtocol extends ILifecycle {
    void query(ByteArray request, ICompletionHandler<ByteArray> responseHandler);

    void updateConfiguration(GroupConfiguration configuration);

    void onLeaderConnected(IMessageSender messageSender, ServerConfiguration serverConfiguration);

    void onLeaderDisconnected();

    void subscribe(UUID subscriptionId, ByteArray request, ICompletionHandler<ByteArray> responseHandler);

    void unsubscribe(UUID subscriptionId);
}
