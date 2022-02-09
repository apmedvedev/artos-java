package com.artos.tests.core.mocks;

import com.artos.impl.core.message.LeaveGroupRequest;
import com.artos.impl.core.message.LeaveGroupResponse;
import com.artos.impl.core.server.membership.ILeaveGroupProtocol;
import com.exametrika.common.utils.ICompletionHandler;

public class LeaveGroupProtocolMock implements ILeaveGroupProtocol {
    private boolean onLeader;
    private boolean onFollower;
    private Throwable error;
    private boolean gracefulClose;
    private boolean leaveGroup;
    private boolean onLeft;

    public boolean isOnLeader() {
        return onLeader;
    }

    public boolean isOnFollower() {
        return onFollower;
    }

    public boolean isGracefulClose() {
        return gracefulClose;
    }

    public boolean isLeaveGroup() {
        return leaveGroup;
    }

    public boolean isOnLeft() {
        return onLeft;
    }

    public void setError(Throwable error) {
        this.error = error;
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
    public void gracefulClose(ICompletionHandler completionHandler) {
        gracefulClose = true;

        if (error == null)
            completionHandler.onSucceeded(null);
        else
            completionHandler.onFailed(error);
    }

    @Override
    public void leaveGroup(ICompletionHandler completionHandler) {
        leaveGroup = true;

        if (error == null)
            completionHandler.onSucceeded(null);
        else
            completionHandler.onFailed(error);
    }

    @Override
    public void onLeft() {
        onLeft = true;
    }

    @Override
    public LeaveGroupResponse handleLeaveGroupRequest(LeaveGroupRequest request) {
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
