package com.artos.tests.core.mocks;

import com.artos.spi.core.IMessageSender;
import com.artos.spi.core.IMessageSenderFactory;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.ICompletionHandler;

public class MessageSenderFactoryMock implements IMessageSenderFactory {
    private MessageSenderMock messageSender = new MessageSenderMock();

    public MessageSenderMock getSender() {
        return messageSender;
    }

    @Override
    public IMessageSender createSender(String endpoint, ICompletionHandler<ByteArray> responseHandler) {
        messageSender.setEndpoint(endpoint);
        messageSender.setResponseHandler(responseHandler);
        return messageSender;
    }
}
