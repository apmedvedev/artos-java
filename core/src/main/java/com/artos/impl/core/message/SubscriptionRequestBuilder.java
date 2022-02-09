/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

import java.util.List;

public class SubscriptionRequestBuilder extends ClientRequestBuilder {
    private List<SubscriptionRequestItem> subscriptions;

    public List<SubscriptionRequestItem> getSubscriptions() {
        return subscriptions;
    }

    public void setSubscriptions(List<SubscriptionRequestItem> subscriptions) {
        this.subscriptions = subscriptions;
    }

    public SubscriptionRequest toMessage() {
        return new SubscriptionRequest(getGroupId(), getSource(), getFlags(), subscriptions);
    }
}
