package com.artos.tests.core.mocks;

import com.artos.impl.core.message.JoinGroupRequest;
import com.artos.impl.core.message.JoinGroupResponse;
import com.artos.impl.core.server.membership.IJoinGroupProtocol;

public class JoinGroupProtocolMock implements IJoinGroupProtocol {
    private boolean onJoined;
    private boolean onLeader;
    private boolean onFollower;

    public boolean isOnJoined() {
        return onJoined;
    }

    public boolean isOnLeader() {
        return onLeader;
    }

    public boolean isOnFollower() {
        return onFollower;
    }

    @Override
    public void onLeader() {
        onLeader = true;
    }

    @Override
    public void onFollower() {
        onFollower = true;
    }

    @Override
    public void onJoined() {
        onJoined = true;
    }

    @Override
    public JoinGroupResponse handleJoinGroupRequest(JoinGroupRequest request) {
        return null;
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
