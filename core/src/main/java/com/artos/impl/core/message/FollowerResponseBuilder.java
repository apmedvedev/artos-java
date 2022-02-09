/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

import java.util.UUID;

public abstract class FollowerResponseBuilder extends ServerResponseBuilder {
    private UUID leaderId;

    public UUID getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(UUID leaderId) {
        this.leaderId = leaderId;
    }
}
