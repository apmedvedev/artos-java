package com.artos.impl.core.server.impl;

import com.artos.api.core.server.conf.ServerConfiguration;
import com.artos.impl.core.message.ServerRequest;
import com.exametrika.common.utils.ILifecycle;

public interface IServerPeer extends ILifecycle {
    ServerConfiguration getConfiguration();

    boolean isConnected();

    boolean isRequestInProgress();

    boolean canHeartbeat(long currentTime);

    long getLastResponseTime();

    long getNextLogIndex();

    void setNextLogIndex(long nextLogIndex);

    long getRewindStep();

    void setRewindStep(long value);

    long getMatchedIndex();

    void setMatchedIndex(long matchedIndex);

    void send(ServerRequest request);
}
