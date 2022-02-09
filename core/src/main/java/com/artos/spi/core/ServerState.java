/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.spi.core;

import java.util.UUID;

public class ServerState {
    private long term;
    private long commitIndex;
    private UUID votedFor;

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public UUID getVotedFor() {
        return votedFor;
    }

    public void setVotedFor(UUID votedFor) {
        this.votedFor = votedFor;
    }

    public void increaseTerm() {
        this.term += 1;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        if (this.commitIndex < commitIndex)
            this.commitIndex = commitIndex;
    }
}
