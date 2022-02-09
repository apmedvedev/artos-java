/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

import java.util.Set;
import java.util.UUID;

public abstract class ServerResponse extends Response {
    private final boolean accepted;

    public ServerResponse(UUID groupId, UUID source, Set<MessageFlags> flags, boolean accepted) {
        super(groupId, source, flags);

        this.accepted = accepted;
    }

    public boolean isAccepted() {
        return accepted;
    }

    @Override
    protected String doToString() {
        return  ", accepted: " + accepted;
    }
}
