/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

import com.exametrika.common.utils.Immutables;

import java.util.List;
import java.util.Set;
import java.util.UUID;

public class SubscriptionResponse extends ClientResponse {
    private final List<SubscriptionResponseItem> results;

    public SubscriptionResponse(UUID groupId, UUID source, Set<MessageFlags> flags, boolean accepted,
                                List<SubscriptionResponseItem> results) {
        super(groupId, source, flags, accepted);

        this.results = Immutables.wrap(results);
    }

    public List<SubscriptionResponseItem> getResults() {
        return results;
    }

    @Override
    protected String doToString() {
        return  (results != null ? ", results: " + results.size() : "") + super.doToString();
    }
}
