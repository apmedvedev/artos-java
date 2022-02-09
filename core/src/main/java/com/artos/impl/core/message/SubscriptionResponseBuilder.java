/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

import java.util.List;

public class SubscriptionResponseBuilder extends ClientResponseBuilder {
    private List<SubscriptionResponseItem> results;

    public List<SubscriptionResponseItem> getResults() {
        return results;
    }

    public void setResults(List<SubscriptionResponseItem> results) {
        this.results = results;
    }

    public SubscriptionResponse toMessage() {
        return new SubscriptionResponse(getGroupId(), getSource(), getFlags(), isAccepted(), results);
    }
}
