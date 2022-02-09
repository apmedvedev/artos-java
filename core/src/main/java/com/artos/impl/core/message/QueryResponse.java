/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

import com.exametrika.common.utils.Immutables;

import java.util.List;
import java.util.Set;
import java.util.UUID;

public class QueryResponse extends ClientResponse {
    private final List<QueryResponseItem> results;

    public QueryResponse(UUID groupId, UUID source, Set<MessageFlags> flags, boolean accepted,
                         List<QueryResponseItem> results) {
        super(groupId, source, flags, accepted);

        this.results = Immutables.wrap(results);
    }

    public List<QueryResponseItem> getResults() {
        return results;
    }

    @Override
    protected String doToString() {
        return  (results != null ? ", results: " + results.size() : "") + super.doToString();
    }
}
