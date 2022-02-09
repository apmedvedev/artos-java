package com.artos.api.shards.client.conf;

import com.artos.impl.shards.client.channel.SimpleHashFunction;
import com.artos.spi.shards.conf.HashFunctionConfiguration;

import java.util.function.ToIntFunction;

public class SimpleHashFunctionConfiguration<K> extends HashFunctionConfiguration {
    @Override
    public ToIntFunction createHashFunction() {
        return new SimpleHashFunction<K>();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof SimpleHashFunctionConfiguration))
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        return 31 * getClass().hashCode();
    }
}
