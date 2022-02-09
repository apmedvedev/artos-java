package com.artos.tests.core.mocks;

import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.impl.core.server.membership.IMembershipProtocol;

public class MembershipProtocolMock implements IMembershipProtocol {
    private GroupConfiguration newConfiguration;
    private boolean committed;
    private boolean leader;
    private boolean follower;

    public GroupConfiguration getNewConfiguration() {
        return newConfiguration;
    }

    public boolean isCommitted() {
        return committed;
    }

    public boolean isOnLeader() {
        return leader;
    }

    public boolean isOnFollower() {
        return follower;
    }

    @Override
    public void onLeader() {
        leader = true;
    }

    @Override
    public void onFollower() {
        follower = true;
    }

    @Override
    public void reconfigure(GroupConfiguration newConfiguration) {
        this.newConfiguration = newConfiguration;
    }

    @Override
    public void commitConfiguration(GroupConfiguration newConfiguration) {
        this.newConfiguration = newConfiguration;
        this.committed = true;
    }
}
