/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

public class VoteResponseBuilder extends ServerResponseBuilder {
    private long term;
    private GroupTransientState transientState;

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public GroupTransientState getTransientState() {
        return transientState;
    }

    public void setTransientState(GroupTransientState transientState) {
        this.transientState = transientState;
    }

    public VoteResponse toMessage() {
        return new VoteResponse(getGroupId(), getSource(), getFlags(), isAccepted(), term, transientState);
    }
}
