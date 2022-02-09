package com.artos.inttests.core.sim.model;

import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.ICompletionHandler;

import java.util.UUID;

public interface ISimQueryChannel {
    void query(ByteArray request, ICompletionHandler<ByteArray> responseHandler);
    void subscribe(UUID subscriptionId, ByteArray request, ICompletionHandler<ByteArray> responseHandler);
    void unsubscribe(UUID subscriptionId);
}
