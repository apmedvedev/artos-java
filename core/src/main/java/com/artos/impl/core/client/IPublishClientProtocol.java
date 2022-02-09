package com.artos.impl.core.client;

import com.artos.impl.core.message.PublishResponse;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.ICompletionHandler;

public interface IPublishClientProtocol {
    long getNextMessageId();

    void publish(ByteArray value, ICompletionHandler<ByteArray> commitHandler);

    void handleAcceptedPublishResponse(PublishResponse response);

    void onLeaderDisconnected();
}
