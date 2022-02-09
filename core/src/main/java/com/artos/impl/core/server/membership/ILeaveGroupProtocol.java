package com.artos.impl.core.server.membership;

import com.artos.impl.core.message.LeaveGroupRequest;
import com.artos.impl.core.message.LeaveGroupResponse;
import com.exametrika.common.compartment.ICompartmentTimerProcessor;
import com.exametrika.common.utils.ICompletionHandler;
import com.exametrika.common.utils.ILifecycle;

public interface ILeaveGroupProtocol extends ILifecycle, ICompartmentTimerProcessor {
    void onLeader();

    void onFollower();

    void gracefulClose(ICompletionHandler completionHandler);

    void leaveGroup(ICompletionHandler completionHandler);

    void onLeft();

    LeaveGroupResponse handleLeaveGroupRequest(LeaveGroupRequest request);
}
