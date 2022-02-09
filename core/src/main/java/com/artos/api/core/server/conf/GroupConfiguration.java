/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.api.core.server.conf;

import com.exametrika.common.config.Configuration;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.Immutables;
import com.exametrika.common.utils.Objects;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class GroupConfiguration extends Configuration {
    private final String name;
    private final UUID groupId;
    private final List<ServerConfiguration> servers;
    private final Map<UUID, ServerConfiguration> serversMap;
    private final boolean singleServer;

    public GroupConfiguration(String name, UUID groupId, List<ServerConfiguration> servers, boolean singleServer) {
        Assert.notNull(name);
        Assert.notNull(groupId);
        Assert.notNull(servers);

        if (singleServer)
            Assert.isTrue(servers.size() == 1);

        this.name = name;
        this.groupId = groupId;
        this.servers = Immutables.wrap(servers);
        this.singleServer = singleServer;

        Map<UUID, ServerConfiguration> serversMap = new HashMap<>();
        for (ServerConfiguration server : servers)
            Assert.isNull(serversMap.put(server.getId(), server));

        this.serversMap = serversMap;
    }

    public String getName() {
        return name;
    }

    public UUID getGroupId() {
        return groupId;
    }

    public List<ServerConfiguration> getServers() {
        return servers;
    }

    public ServerConfiguration findServer(UUID id) {
        Assert.notNull(id);

        return serversMap.get(id);
    }

    public boolean isSingleServer() {
        return singleServer;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof GroupConfiguration))
            return false;

        GroupConfiguration configuration = (GroupConfiguration) o;
        return name.equals(configuration.name) && groupId.equals(configuration.groupId) && servers.equals(configuration.servers) &&
            singleServer == configuration.singleServer;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, groupId, servers, singleServer);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (ServerConfiguration configuration : servers) {
            if (first)
                first = false;
            else
                builder.append(", ");

            builder.append(configuration.toString());
        }

        return name + "@" + groupId + "[" + builder.toString() + "]";
    }
}
