package com.artos.tests.core.mocks;

import com.artos.spi.core.IMessageReceiver;
import com.artos.spi.core.IMessageListener;
import com.artos.spi.core.IMessageListenerFactory;

public class MessageListenerFactoryMock implements IMessageListener, IMessageListenerFactory {
    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public IMessageListener createListener(IMessageReceiver messageReceiver) {
        return this;
    }
}
