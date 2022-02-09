package com.artos.impl.core.message;

import com.exametrika.common.utils.Assert;

import java.util.UUID;

public class ClientSessionTransientState {
    private final UUID clientId;
    private final long lastCommittedMessageId;

    public ClientSessionTransientState(UUID clientId, long lastCommittedMessageId) {
        Assert.notNull(clientId);

        this.clientId = clientId;
        this.lastCommittedMessageId = lastCommittedMessageId;
    }

    public UUID getClientId() {
        return clientId;
    }

    public long getLastCommittedMessageId() {
        return lastCommittedMessageId;
    }

    @Override
    public String toString() {
        return clientId + ":" + lastCommittedMessageId;
    }
}
