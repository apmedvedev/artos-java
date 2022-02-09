package com.artos.spi.core.conf;

import com.artos.spi.core.ILogStoreFactory;
import com.exametrika.common.config.Configuration;

public abstract class LogStoreFactoryConfiguration extends Configuration {
    public abstract ILogStoreFactory createFactory();
}
