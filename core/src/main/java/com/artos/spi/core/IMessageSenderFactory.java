/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.spi.core;

import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.ICompletionHandler;

public interface IMessageSenderFactory {

    /**
     * Creates a message sender for the given endpoint
     * @param endpoint endpoint for the server
     * @param responseHandler response handler
     * @return an instance of message sender
     */
    IMessageSender createSender(String endpoint, ICompletionHandler<ByteArray> responseHandler);
}
