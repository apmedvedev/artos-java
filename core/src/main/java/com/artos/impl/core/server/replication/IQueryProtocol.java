package com.artos.impl.core.server.replication;

import com.artos.impl.core.message.QueryRequest;
import com.artos.impl.core.message.QueryResponse;
import com.artos.impl.core.message.SubscriptionRequest;
import com.artos.impl.core.message.SubscriptionResponse;
import com.exametrika.common.compartment.ICompartmentTimerProcessor;
import com.exametrika.common.utils.ICompletionHandler;
import com.exametrika.common.utils.ILifecycle;

public interface IQueryProtocol extends ILifecycle, ICompartmentTimerProcessor {
    void onCommit(ClientSession session);

    void onFollower();

    void handleQueryRequest(QueryRequest request, ICompletionHandler<QueryResponse> response);

    void handleSubscriptionRequest(SubscriptionRequest request, ICompletionHandler<SubscriptionResponse> response);

    void onSessionRemoved(ClientSession session);
}
