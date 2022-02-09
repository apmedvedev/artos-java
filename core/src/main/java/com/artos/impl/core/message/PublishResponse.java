/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

import com.artos.api.core.server.conf.GroupConfiguration;

import java.util.Set;
import java.util.UUID;

public class PublishResponse extends ClientResponse {
    private final UUID leaderId;
    private final long lastReceivedMessageId;
    private final long lastCommittedMessageId;
    private final GroupConfiguration configuration;

    public PublishResponse(UUID groupId, UUID source, UUID leaderId, Set<MessageFlags> flags,
                           boolean accepted, long lastReceivedMessageId, long lastCommittedMessageId,
                           GroupConfiguration configuration) {
        super(groupId, source, flags, accepted);

        this.leaderId = leaderId;
        this.lastReceivedMessageId = lastReceivedMessageId;
        this.lastCommittedMessageId = lastCommittedMessageId;
        this.configuration = configuration;
    }

    public UUID getLeaderId() {
        return leaderId;
    }

    public long getLastReceivedMessageId() {
        return lastReceivedMessageId;
    }

    public long getLastCommittedMessageId() {
        return lastCommittedMessageId;
    }

    public GroupConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    protected String doToString() {
        return  ", leader:" + leaderId + ", last-received-message-id: " + lastReceivedMessageId +
            ", last-committed-message-id: " + lastCommittedMessageId + (configuration != null ? ", configuration: " + configuration : "") + super.doToString();
    }
}
