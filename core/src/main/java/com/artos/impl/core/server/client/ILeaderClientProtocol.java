package com.artos.impl.core.server.client;

import com.exametrika.common.compartment.ICompartmentTimerProcessor;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.ICompletionHandler;

public interface ILeaderClientProtocol extends ICompartmentTimerProcessor {
    void publish(ByteArray value, ICompletionHandler<ByteArray> commitHandler);

    void onLeader();

    void onFollower();
}
