/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

public class VoteRequestBuilder extends ServerRequestBuilder {
    private long term;
    private long lastLogTerm;
    private long lastLogIndex;

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

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

    public VoteRequest toMessage() {
        return new VoteRequest(getGroupId(), getSource(), getFlags(),
            term, lastLogTerm, lastLogIndex);
    }
}
