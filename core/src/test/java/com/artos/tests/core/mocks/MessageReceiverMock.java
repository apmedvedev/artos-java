package com.artos.tests.core.mocks;

import com.artos.spi.core.IMessageReceiver;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.ICompletionHandler;

public class MessageReceiverMock implements IMessageReceiver {
    private ByteArray request;
    private ByteArray response;

    public ByteArray getRequest() {
        return request;
    }

    public void setResponse(ByteArray response) {
        this.response = response;
    }

    @Override
    public void receive(ByteArray request, ICompletionHandler<ByteArray> response) {
        this.request = request;
        response.onSucceeded(this.response);
    }
}
