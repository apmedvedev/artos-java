package com.artos.impl.core.server.election;

import com.artos.impl.core.message.VoteRequest;
import com.artos.impl.core.message.VoteResponse;
import com.exametrika.common.compartment.ICompartmentTimerProcessor;
import com.exametrika.common.utils.ILifecycle;

public interface IElectionProtocol extends ILifecycle, ICompartmentTimerProcessor {
    void delayElection();

    void becomeFollower();

    void lockElection();

    VoteResponse handleVoteRequest(VoteRequest request);

    void handleVoteResponse(VoteResponse response);
}
