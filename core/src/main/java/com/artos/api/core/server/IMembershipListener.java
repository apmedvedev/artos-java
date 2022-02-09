package com.artos.api.core.server;

public interface IMembershipListener {
    void onLeader();

    void onFollower();

    void onJoined();

    void onLeft();

    void onMembershipChanged(MembershipChangeEvent event);
}
