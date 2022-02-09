package com.artos.impl.core.server.state;

import com.exametrika.common.utils.ICompletionHandler;
import com.exametrika.common.utils.ILifecycle;

public interface IStateTransferProtocol extends ILifecycle {
    void requestStateTransfer(String host, int port, long startLogIndex, ICompletionHandler completionHandler);

    void onLeader();

    void onFollower();

    String getHost();

    int getPort();
}
