package com.artos.tests.core.mocks;

import com.artos.spi.core.IMessageSender;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.ICompletionHandler;

public class MessageSenderMock implements IMessageSender {
    private ICompletionHandler<ByteArray> responseHandler;
    private boolean failure;
    private ByteArray response;
    private Throwable error;
    private ByteArray request;
    private String endpoint;

    public void setResponseHandler(ICompletionHandler<ByteArray> responseHandler) {
        this.responseHandler = responseHandler;
    }

    public ByteArray getRequest() {
        return request;
    }

    public void setFailure(boolean failure) {
        this.failure = failure;
    }

    public void setResponse(ByteArray response) {
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
    public void send(ByteArray request) {
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
