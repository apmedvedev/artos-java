/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

import com.exametrika.common.utils.Immutables;

import java.util.List;
import java.util.Set;
import java.util.UUID;

public class SubscriptionRequest extends ClientRequest {
    private final List<SubscriptionRequestItem> subscriptions;

    public SubscriptionRequest(UUID groupId, UUID source, Set<MessageFlags> flags,
                               List<SubscriptionRequestItem> subscriptions) {
        super(groupId, source, flags);

        this.subscriptions = Immutables.wrap(subscriptions);
    }

    public List<SubscriptionRequestItem> getSubscriptions() {
        return subscriptions;
    }

    @Override
    protected String doToString() {
        return  (subscriptions != null ? ", subscriptions: " + subscriptions.size() : "") + super.doToString();
    }
}
