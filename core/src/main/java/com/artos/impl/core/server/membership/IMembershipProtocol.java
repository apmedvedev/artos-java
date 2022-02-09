package com.artos.impl.core.server.membership;

import com.artos.api.core.server.conf.GroupConfiguration;

public interface IMembershipProtocol {
    void onLeader();

    void onFollower();

    void reconfigure(GroupConfiguration newConfiguration);

    void commitConfiguration(GroupConfiguration newConfiguration);
}
