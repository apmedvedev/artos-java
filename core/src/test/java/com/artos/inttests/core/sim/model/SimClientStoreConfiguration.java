package com.artos.inttests.core.sim.model;

import com.artos.spi.core.IClientStore;
import com.artos.spi.core.conf.ClientStoreConfiguration;

public class SimClientStoreConfiguration extends ClientStoreConfiguration {
    private final SimClientStore clientStore;

    public SimClientStoreConfiguration(SimClientStore clientStore) {
        this.clientStore = clientStore;
    }

    @Override
    public IClientStore createClientStore() {
        return clientStore;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof SimClientStoreConfiguration))
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        return 31 * getClass().hashCode();
    }
}
