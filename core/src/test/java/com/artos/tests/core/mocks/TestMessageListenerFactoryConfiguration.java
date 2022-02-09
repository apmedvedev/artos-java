package com.artos.tests.core.mocks;

import com.artos.spi.core.IMessageListenerFactory;
import com.artos.spi.core.conf.MessageListenerFactoryConfiguration;

public class TestMessageListenerFactoryConfiguration extends MessageListenerFactoryConfiguration {
    @Override
    public IMessageListenerFactory createFactory() {
        return new MessageListenerFactoryMock();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof TestMessageListenerFactoryConfiguration))
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        return 31 * getClass().hashCode();
    }
}
