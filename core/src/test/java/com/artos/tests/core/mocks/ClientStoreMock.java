package com.artos.tests.core.mocks;

import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.spi.core.IClientStore;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ClientStoreMock implements IClientStore {
    private Map<UUID, GroupConfiguration> configurations = new HashMap<>();

    public Map<UUID, GroupConfiguration> getConfigurations() {
        return configurations;
    }

    public void setConfigurations(Map<UUID, GroupConfiguration> configurations) {
        this.configurations = configurations;
    }

    @Override
    public GroupConfiguration loadGroupConfiguration(UUID groupId) {
        return configurations.get(groupId);
    }

    @Override
    public void saveGroupConfiguration(GroupConfiguration configuration) {
        configurations.put(configuration.getGroupId(), configuration);
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }
}
