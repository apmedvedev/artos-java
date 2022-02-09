/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

import java.util.Set;
import java.util.UUID;

public class VoteRequest extends ServerRequest {
    private final long term;
    private final long lastLogTerm;
    private final long lastLogIndex;

    public VoteRequest(UUID groupId, UUID source, Set<MessageFlags> flags,
                       long term, long lastLogTerm, long lastLogIndex) {
        super(groupId, source, flags);

        this.term = term;
        this.lastLogTerm = lastLogTerm;
        this.lastLogIndex = lastLogIndex;
    }

    public long getTerm() {
        return term;
    }

    public long getLastLogTerm() {
        return lastLogTerm;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    @Override
    protected String doToString() {
        return  ", term: " + term + ", last-log-term: " + lastLogTerm + ", last-log-index: " + lastLogIndex + super.doToString();
    }
}
