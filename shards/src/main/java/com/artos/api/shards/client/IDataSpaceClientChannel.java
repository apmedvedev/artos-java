package com.artos.api.shards.client;

import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.ICompletionHandler;
import com.exametrika.common.utils.ILifecycle;

import java.util.UUID;

public interface IDataSpaceClientChannel<K> extends ILifecycle {
    boolean isStarted();

    void publish(K key, ByteArray value, ICompletionHandler<ByteArray> commitHandler);

    void query(K key, ByteArray request, ICompletionHandler<ByteArray> responseHandler);

    void subscribe(K key, UUID subscriptionId, ByteArray request, ICompletionHandler<ByteArray> responseHandler);

    void unsubscribe(K key, UUID subscriptionId);

}
