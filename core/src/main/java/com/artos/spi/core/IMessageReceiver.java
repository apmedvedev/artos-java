/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.spi.core;

import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.ICompletionHandler;

public interface IMessageReceiver {
    void receive(ByteArray request, ICompletionHandler<ByteArray> response);
}
