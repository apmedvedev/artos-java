/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

public class AppendEntriesResponseBuilder extends ServerResponseBuilder {
    private long term;
    private long nextIndex;

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public long getNextIndex() {
        return nextIndex;
    }

    public void setNextIndex(long nextIndex) {
        this.nextIndex = nextIndex;
    }

    public AppendEntriesResponse toMessage() {
        return new AppendEntriesResponse(getGroupId(), getSource(), getFlags(), isAccepted(), term, nextIndex);
    }
}
