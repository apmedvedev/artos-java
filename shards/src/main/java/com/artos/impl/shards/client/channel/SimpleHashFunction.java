package com.artos.impl.shards.client.channel;

import java.util.function.ToIntFunction;

public class SimpleHashFunction<K> implements ToIntFunction<K> {
    @Override
    public int applyAsInt(K value) {
        return value.hashCode();
    }
}
