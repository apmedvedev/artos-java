/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

import com.artos.api.core.server.conf.ServerConfiguration;
import com.exametrika.common.utils.Assert;

import java.util.Set;
import java.util.UUID;

public class JoinGroupRequest extends FollowerRequest {
    private final long lastLogTerm;
    private final long lastLogIndex;
    private final ServerConfiguration configuration;

    public JoinGroupRequest(UUID groupId, UUID source, Set<MessageFlags> flags,
                            long lastLogTerm, long lastLogIndex, ServerConfiguration configuration) {
        super(groupId, source, flags);

        Assert.notNull(configuration);

        this.lastLogTerm = lastLogTerm;
        this.lastLogIndex = lastLogIndex;
        this.configuration = configuration;
    }

    public long getLastLogTerm() {
        return lastLogTerm;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public ServerConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    protected String doToString() {
        return  ", last-log-term: " + lastLogTerm + ", last-log-index: " + lastLogIndex + ", configuration: " + configuration + super.doToString();
    }
}
