package com.artos.api.core.server;

import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.api.core.server.conf.ServerConfiguration;

import java.util.UUID;

public interface IMembershipService {
    ServerConfiguration getLocalServer();

    GroupConfiguration getGroup();

    UUID getLeader();

    void addMembershipListener(IMembershipListener listener);

    void removeMembershipListener(IMembershipListener listener);

    void removeAllMembershipListeners();
}
