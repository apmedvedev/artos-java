/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

import java.util.Set;
import java.util.UUID;

public class VoteResponse extends ServerResponse {
    private final long term;
    private final GroupTransientState transientState;

    public VoteResponse(UUID groupId, UUID source, Set<MessageFlags> flags, boolean accepted, long term, GroupTransientState transientState) {
        super(groupId, source, flags, accepted);

        this.term = term;
        this.transientState = transientState;
    }

    public long getTerm() {
        return term;
    }

    public GroupTransientState getTransientState() {
        return transientState;
    }

    @Override
    protected String doToString() {
        return  ", term: " + term + (transientState != null ? ", transient-state: " + transientState.toString() : "") + super.doToString();
    }
}
