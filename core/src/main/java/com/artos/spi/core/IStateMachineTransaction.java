/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.spi.core;

import com.artos.api.core.server.conf.GroupConfiguration;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.ICompletionHandler;

import java.util.UUID;

public interface IStateMachineTransaction {

    boolean isReadOnly();

    GroupConfiguration readConfiguration();

    void writeConfiguration(GroupConfiguration configuration);

    ServerState readState();

    void writeState(ServerState serverState);

    void publish(long logIndex, ByteArray data);

    void query(ByteArray request, ICompletionHandler<ByteArray> responseHandler);

    void subscribe(UUID subscriptionId, ByteArray request, ICompletionHandler<ByteArray> responseHandler);

    void unsubscribe(UUID subscriptionId);

    void commit();
}
