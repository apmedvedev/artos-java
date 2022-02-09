package com.artos.spi.shards.conf;

import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.Objects;

public abstract class DataSpaceClientChannelConfiguration<K> {
    private final String endpoint;
    private final HashFunctionConfiguration<K> hashFunction;

    public DataSpaceClientChannelConfiguration(String endpoint, HashFunctionConfiguration<K> hashFunction) {
        Assert.notNull(hashFunction);

        this.endpoint = endpoint;
        this.hashFunction = hashFunction;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public HashFunctionConfiguration<K> getHashFunction() {
        return hashFunction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof DataSpaceClientChannelConfiguration))
            return false;

        DataSpaceClientChannelConfiguration configuration = (DataSpaceClientChannelConfiguration) o;
        return hashFunction.equals(configuration.hashFunction);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(hashFunction);
    }
}
