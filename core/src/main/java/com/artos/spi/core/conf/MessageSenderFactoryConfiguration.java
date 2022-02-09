package com.artos.spi.core.conf;

import com.artos.spi.core.IMessageSenderFactory;
import com.exametrika.common.config.Configuration;

public abstract class MessageSenderFactoryConfiguration extends Configuration {
    public abstract IMessageSenderFactory createFactory();
}
