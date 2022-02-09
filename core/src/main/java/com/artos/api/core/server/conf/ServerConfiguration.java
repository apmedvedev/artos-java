/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.api.core.server.conf;

import com.exametrika.common.config.Configuration;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.Objects;

import java.util.UUID;

public class ServerConfiguration extends Configuration {
    private final UUID id;
    private final String endpoint;

    public ServerConfiguration(UUID id, String endpoint) {
        Assert.notNull(id);
        Assert.notNull(endpoint);

        this.id = id;
        this.endpoint = endpoint;
    }

    public UUID getId() {
        return id;
    }

    public String getEndpoint() {
        return endpoint;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof ServerConfiguration))
            return false;

        ServerConfiguration configuration = (ServerConfiguration) o;
        return id.equals(configuration.id) && endpoint.equals(configuration.endpoint);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id, endpoint);
    }

    @Override
    public String toString() {
        return endpoint + ":" + id;
    }
}
