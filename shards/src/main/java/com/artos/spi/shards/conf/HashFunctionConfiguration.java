package com.artos.spi.shards.conf;

import com.exametrika.common.config.Configuration;

import java.util.function.ToIntFunction;

public abstract class HashFunctionConfiguration<K> extends Configuration {
    public abstract ToIntFunction<K> createHashFunction();
}
