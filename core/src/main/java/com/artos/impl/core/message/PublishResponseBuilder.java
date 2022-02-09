/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

import com.artos.api.core.server.conf.GroupConfiguration;

import java.util.UUID;

public class PublishResponseBuilder extends ClientResponseBuilder {
    private UUID leaderId;
    private long lastReceivedMessageId;
    private long lastCommittedMessageId;
    private GroupConfiguration configuration;

    public UUID getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(UUID leaderId) {
        this.leaderId = leaderId;
    }

    public long getLastReceivedMessageId() {
        return lastReceivedMessageId;
    }

    public void setLastReceivedMessageId(long lastReceivedMessageId) {
        this.lastReceivedMessageId = lastReceivedMessageId;
    }

    public long getLastCommittedMessageId() {
        return lastCommittedMessageId;
    }

    public void setLastCommittedMessageId(long lastCommittedMessageId) {
        this.lastCommittedMessageId = lastCommittedMessageId;
    }

    public GroupConfiguration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(GroupConfiguration configuration) {
        this.configuration = configuration;
    }

    public PublishResponse toMessage() {
        return new PublishResponse(getGroupId(), getSource(), getLeaderId(), getFlags(),
            isAccepted(), lastReceivedMessageId, lastCommittedMessageId, configuration);
    }
}
