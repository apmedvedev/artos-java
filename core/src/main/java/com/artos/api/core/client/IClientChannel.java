package com.artos.api.core.client;

import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.ICompletionHandler;
import com.exametrika.common.utils.ILifecycle;

import java.util.UUID;

public interface IClientChannel extends ILifecycle {
    boolean isStarted();

    void publish(ByteArray value, ICompletionHandler<ByteArray> commitHandler);

    void query(ByteArray request, ICompletionHandler<ByteArray> responseHandler);

    void subscribe(UUID subscriptionId, ByteArray request, ICompletionHandler<ByteArray> responseHandler);

    void unsubscribe(UUID subscriptionId);
}
