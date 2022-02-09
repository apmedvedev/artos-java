/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

import java.util.List;

public class QueryRequestBuilder extends ClientRequestBuilder {
    private List<QueryRequestItem> queries;

    public List<QueryRequestItem> getQueries() {
        return queries;
    }

    public void setQueries(List<QueryRequestItem> queries) {
        this.queries = queries;
    }

    public QueryRequest toMessage() {
        return new QueryRequest(getGroupId(), getSource(), getFlags(), queries);
    }
}
