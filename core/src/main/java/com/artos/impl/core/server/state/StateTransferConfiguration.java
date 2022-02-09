package com.artos.impl.core.server.state;

import com.exametrika.common.config.Configuration;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.Objects;

public class StateTransferConfiguration extends Configuration {
    private String host;
    private int port;
    private long startLogIndex;

    public StateTransferConfiguration(String host, int port, long startLogIndex) {
        Assert.notNull(host);

        this.host = host;
        this.port = port;
        this.startLogIndex = startLogIndex;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public long getStartLogIndex() {
        return startLogIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof StateTransferConfiguration))
            return false;

        StateTransferConfiguration configuration = (StateTransferConfiguration) o;
        return host.equals(configuration.host) && port == configuration.port && startLogIndex == configuration.startLogIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(host, port, startLogIndex);
    }

    @Override
    public String toString() {
        return host + ":" + port + "@" + startLogIndex;
    }
}
