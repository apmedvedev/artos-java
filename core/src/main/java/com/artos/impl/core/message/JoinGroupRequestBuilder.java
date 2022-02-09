/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

import com.artos.api.core.server.conf.ServerConfiguration;

public class JoinGroupRequestBuilder extends FollowerRequestBuilder {
    private long lastLogTerm;
    private long lastLogIndex;
    private ServerConfiguration configuration;

    public long getLastLogTerm() {
        return lastLogTerm;
    }

    public void setLastLogTerm(long lastLogTerm) {
        this.lastLogTerm = lastLogTerm;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public void setLastLogIndex(long lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
    }

    public ServerConfiguration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(ServerConfiguration configuration) {
        this.configuration = configuration;
    }

    public JoinGroupRequest toMessage() {
        return new JoinGroupRequest(getGroupId(), getSource(), getFlags(),
            lastLogTerm, lastLogIndex, configuration);
    }
}
