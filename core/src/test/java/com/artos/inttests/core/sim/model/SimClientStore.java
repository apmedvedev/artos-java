package com.artos.inttests.core.sim.model;

import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.spi.core.IClientStore;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class SimClientStore implements IClientStore {
    private Map<UUID, GroupConfiguration> configurations = new HashMap<>();

    @Override
    public synchronized GroupConfiguration loadGroupConfiguration(UUID groupId) {
        return configurations.get(groupId);
    }

    @Override
    public synchronized void saveGroupConfiguration(GroupConfiguration configuration) {
        configurations.put(configuration.getGroupId(), configuration);
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }
}
