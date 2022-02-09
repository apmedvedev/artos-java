/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.spi.core;

public interface IMessageListenerFactory {
    IMessageListener createListener(IMessageReceiver messageReceiver);
}
