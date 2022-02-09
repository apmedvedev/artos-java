package com.artos.api.core.server;

import com.artos.api.core.server.conf.ServerConfiguration;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.Immutables;

import java.util.List;
import java.util.UUID;

public class MembershipChange {
    private final List<ServerConfiguration> addedServers;
    private final List<UUID> removedServers;

    public MembershipChange(List<ServerConfiguration> addedServers, List<UUID> removedServers) {
        Assert.notNull(addedServers);
        Assert.notNull(removedServers);

        this.addedServers = Immutables.wrap(addedServers);
        this.removedServers = Immutables.wrap(removedServers);
    }

    public List<ServerConfiguration> getAddedServers() {
        return addedServers;
    }

    public List<UUID> getRemovedServers() {
        return removedServers;
    }

    @Override
    public String toString() {
        return "+" + addedServers + ", -" + removedServers;
    }
}
