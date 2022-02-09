package com.artos.impl.core.server.channel;

import com.artos.api.core.server.IMembershipListener;
import com.artos.api.core.server.IMembershipService;
import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.api.core.server.conf.ServerConfiguration;
import com.artos.impl.core.server.client.ILeaderClientProtocol;
import com.artos.spi.core.IGroupChannel;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.ICompletionHandler;

import java.util.UUID;

public class GroupChannel implements IGroupChannel {
    private final ILeaderClientProtocol leaderClientProtocol;
    private final IMembershipService membershipService;

    public GroupChannel(ILeaderClientProtocol leaderClientProtocol, IMembershipService membershipService) {
        Assert.notNull(leaderClientProtocol);
        Assert.notNull(membershipService);

        this.leaderClientProtocol = leaderClientProtocol;
        this.membershipService = membershipService;
    }

    @Override
    public void publish(ByteArray value, ICompletionHandler<ByteArray> commitHandler) {
        leaderClientProtocol.publish(value, commitHandler);
    }

    @Override
    public ServerConfiguration getLocalServer() {
        return membershipService.getLocalServer();
    }

    @Override
    public GroupConfiguration getGroup() {
        return membershipService.getGroup();
    }

    @Override
    public UUID getLeader() {
        return membershipService.getLeader();
    }

    @Override
    public void addMembershipListener(IMembershipListener listener) {
        membershipService.addMembershipListener(listener);
    }

    @Override
    public void removeMembershipListener(IMembershipListener listener) {
        membershipService.removeMembershipListener(listener);
    }

    @Override
    public void removeAllMembershipListeners() {
        membershipService.removeAllMembershipListeners();
    }
}
