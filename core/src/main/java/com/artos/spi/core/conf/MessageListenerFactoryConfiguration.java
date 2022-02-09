package com.artos.spi.core.conf;

import com.artos.spi.core.IMessageListenerFactory;
import com.exametrika.common.config.Configuration;

public abstract class MessageListenerFactoryConfiguration extends Configuration {
    public abstract IMessageListenerFactory createFactory();
}
