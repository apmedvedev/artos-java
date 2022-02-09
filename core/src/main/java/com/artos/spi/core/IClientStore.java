package com.artos.spi.core;

import com.artos.api.core.server.conf.GroupConfiguration;
import com.exametrika.common.utils.ILifecycle;

import java.util.UUID;

public interface IClientStore extends ILifecycle {
    GroupConfiguration loadGroupConfiguration(UUID groupId);

    void saveGroupConfiguration(GroupConfiguration configuration);
}
