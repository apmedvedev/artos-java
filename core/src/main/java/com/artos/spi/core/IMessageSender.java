/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.spi.core;

import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.ILifecycle;

public interface IMessageSender extends ILifecycle {
    void send(ByteArray request);
}
