package com.artos.tests.core.mocks;

import com.artos.impl.core.message.FollowerRequest;
import com.artos.impl.core.message.FollowerResponse;
import com.artos.impl.core.server.client.IFollowerMessageSender;
import com.exametrika.common.utils.ICompletionHandler;

public class FollowerMessageSenderMock implements IFollowerMessageSender {
    private boolean failure;
    private FollowerResponse response;
    private Throwable error;
    private FollowerRequest request;
    private String endpoint;
    private ICompletionHandler<FollowerResponse> responseHandler;

    public FollowerRequest getRequest() {
        return request;
    }

    public void setFailure(boolean failure) {
        this.failure = failure;
    }

    public void setResponse(FollowerResponse response) {
        this.response = response;
    }

    public void setError(Throwable error) {
        this.error = error;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public void send(FollowerRequest request) {
        this.request = request;
        if (!failure && response != null)
            responseHandler.onSucceeded(response);
        else if (failure && error != null)
            responseHandler.onFailed(error);
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }
}
