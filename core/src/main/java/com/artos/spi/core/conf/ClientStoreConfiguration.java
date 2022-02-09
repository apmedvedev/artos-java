package com.artos.spi.core.conf;

import com.artos.spi.core.IClientStore;
import com.exametrika.common.config.Configuration;

public abstract class ClientStoreConfiguration extends Configuration {
    public abstract IClientStore createClientStore();
}
