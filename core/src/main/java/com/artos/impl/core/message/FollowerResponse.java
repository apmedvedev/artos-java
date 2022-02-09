/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

import java.util.Set;
import java.util.UUID;

public abstract class FollowerResponse extends ServerResponse {
    private final UUID leaderId;

    public FollowerResponse(UUID groupId, UUID source, Set<MessageFlags> flags, boolean accepted, UUID leaderId) {
        super(groupId, source, flags, accepted);

        this.leaderId = leaderId;
    }

    public UUID getLeaderId() {
        return leaderId;
    }

    @Override
    protected String doToString() {
        return  ", leader-id: " + leaderId + super.doToString();
    }
}
