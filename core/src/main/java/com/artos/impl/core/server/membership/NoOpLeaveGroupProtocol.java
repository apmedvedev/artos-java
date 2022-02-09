package com.artos.impl.core.server.membership;

import com.artos.impl.core.message.LeaveGroupRequest;
import com.artos.impl.core.message.LeaveGroupResponse;
import com.exametrika.common.l10n.DefaultMessage;
import com.exametrika.common.l10n.ILocalizedMessage;
import com.exametrika.common.l10n.Messages;
import com.exametrika.common.l10n.SystemException;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.ICompletionHandler;

public class NoOpLeaveGroupProtocol implements ILeaveGroupProtocol {
    private static final IMessages messages = Messages.get(IMessages.class);

    @Override
    public void onLeader() {
    }

    @Override
    public void onFollower() {
    }

    @Override
    public void gracefulClose(ICompletionHandler completionHandler) {
        completionHandler.onSucceeded(true);
    }

    @Override
    public void leaveGroup(ICompletionHandler completionHandler) {
        completionHandler.onFailed(new SystemException(messages.leaveGroupSingleServer()));
    }

    @Override
    public void onLeft() {
    }

    @Override
    public LeaveGroupResponse handleLeaveGroupRequest(LeaveGroupRequest request) {
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

    private interface IMessages {
        @DefaultMessage("Could not leave group. Group consists of single server.")
        ILocalizedMessage leaveGroupSingleServer();
    }
}
