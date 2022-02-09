package com.artos.tests.core.mocks;

import com.artos.spi.core.IMessageSenderFactory;
import com.artos.spi.core.conf.MessageSenderFactoryConfiguration;

public class TestMessageSenderFactoryConfiguration extends MessageSenderFactoryConfiguration {
    @Override
    public IMessageSenderFactory createFactory() {
        return new MessageSenderFactoryMock();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof TestMessageSenderFactoryConfiguration))
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        return 31 * getClass().hashCode();
    }
}
