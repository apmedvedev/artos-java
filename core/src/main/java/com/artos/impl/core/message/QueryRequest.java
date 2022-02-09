/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

import com.exametrika.common.utils.Immutables;

import java.util.List;
import java.util.Set;
import java.util.UUID;

public class QueryRequest extends ClientRequest {
    private final List<QueryRequestItem> queries;

    public QueryRequest(UUID groupId, UUID source, Set<MessageFlags> flags,
                        List<QueryRequestItem> queries) {
        super(groupId, source, flags);

        this.queries = Immutables.wrap(queries);
    }

    public List<QueryRequestItem> getQueries() {
        return queries;
    }

    @Override
    protected String doToString() {
        return  (queries != null ? ", queries: " + queries.size() : "") + super.doToString();
    }
}
