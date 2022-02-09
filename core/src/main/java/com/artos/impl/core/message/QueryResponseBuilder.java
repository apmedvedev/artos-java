/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

import java.util.List;

public class QueryResponseBuilder extends ClientResponseBuilder {
    private List<QueryResponseItem> results;

    public List<QueryResponseItem> getResults() {
        return results;
    }

    public void setResults(List<QueryResponseItem> results) {
        this.results = results;
    }

    public QueryResponse toMessage() {
        return new QueryResponse(getGroupId(), getSource(), getFlags(), isAccepted(), results);
    }
}
