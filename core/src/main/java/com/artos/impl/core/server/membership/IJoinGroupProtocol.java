package com.artos.impl.core.server.membership;

import com.artos.impl.core.message.JoinGroupRequest;
import com.artos.impl.core.message.JoinGroupResponse;
import com.exametrika.common.compartment.ICompartmentTimerProcessor;
import com.exametrika.common.utils.ILifecycle;

public interface IJoinGroupProtocol extends ILifecycle, ICompartmentTimerProcessor {
    void onLeader();

    void onFollower();

    void onJoined();

    JoinGroupResponse handleJoinGroupRequest(JoinGroupRequest request);
}
