package com.artos.tests.core.mocks;

import com.artos.impl.core.message.VoteRequest;
import com.artos.impl.core.message.VoteResponse;
import com.artos.impl.core.server.election.IElectionProtocol;
import com.artos.impl.core.server.impl.ServerRole;
import com.artos.impl.core.server.impl.State;

public class ElectionProtocolMock implements IElectionProtocol {
    private boolean electionTimerRestarted;
    private ServerRole serverRole;
    private State state;

    public boolean isElectionTimerRestarted() {
        return electionTimerRestarted;
    }

    public ServerRole getServerRole() {
        return serverRole;
    }

    public void setState(State state) {
        this.state = state;
    }

    @Override
    public void delayElection() {
        electionTimerRestarted = true;
    }

    @Override
    public void becomeFollower() {
        serverRole = ServerRole.FOLLOWER;
        if (state != null)
            state.setRole(serverRole);
    }

    @Override
    public void lockElection() {
    }

    @Override
    public VoteResponse handleVoteRequest(VoteRequest request) {
        return null;
    }

    @Override
    public void handleVoteResponse(VoteResponse response) {
    }

    @Override
    public void onTimer(long currentTime) {
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }
}
