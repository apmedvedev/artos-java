package com.artos.impl.core.server.membership;

import com.artos.impl.core.message.JoinGroupRequest;
import com.artos.impl.core.message.JoinGroupResponse;
import com.exametrika.common.utils.Assert;

public class NoOpJoinGroupProtocol implements IJoinGroupProtocol {
    @Override
    public void onLeader() {
    }

    @Override
    public void onFollower() {
    }

    @Override
    public void onJoined() {
    }

    @Override
    public JoinGroupResponse handleJoinGroupRequest(JoinGroupRequest request) {
        Assert.supports(false);
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
