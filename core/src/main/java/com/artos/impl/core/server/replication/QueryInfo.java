package com.artos.impl.core.server.replication;

import com.artos.impl.core.message.QueryRequestItem;
import com.artos.impl.core.message.QueryResponse;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.ICompletionHandler;

public class QueryInfo {
    private final QueryRequestItem query;
    private final ICompletionHandler<QueryResponse> responseHandler;

    public QueryInfo(QueryRequestItem query, ICompletionHandler<QueryResponse> responseHandler) {
        Assert.notNull(query);
        Assert.notNull(responseHandler);

        this.query = query;
        this.responseHandler = responseHandler;
    }

    public QueryRequestItem getQuery() {
        return query;
    }

    public ICompletionHandler<QueryResponse> getResponseHandler() {
        return responseHandler;
    }

    @Override
    public String toString() {
        return query.toString();
    }
}
