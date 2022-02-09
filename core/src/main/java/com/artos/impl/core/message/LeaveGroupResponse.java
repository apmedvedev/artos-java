/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

import com.artos.api.core.server.conf.GroupConfiguration;

import java.util.Set;
import java.util.UUID;

public class LeaveGroupResponse extends FollowerResponse {
    private final GroupConfiguration configuration;

    public LeaveGroupResponse(UUID groupId, UUID source, Set<MessageFlags> flags, boolean accepted, UUID leaderId,
                              GroupConfiguration configuration) {
        super(groupId, source, flags, accepted, leaderId);

        this.configuration = configuration;
    }

    public GroupConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    protected String doToString() {
        return  ", configuration: " + configuration + super.doToString();
    }
}
