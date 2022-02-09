/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

import java.util.Set;
import java.util.UUID;

public class AppendEntriesResponse extends ServerResponse {
    private final long term;
    private final long nextIndex;

    public AppendEntriesResponse(UUID groupId, UUID source, Set<MessageFlags> flags, boolean accepted,
                                 long term, long nextIndex) {
        super(groupId, source, flags, accepted);

        this.term = term;
        this.nextIndex = nextIndex;
    }

    public long getTerm() {
        return term;
    }

    public long getNextIndex() {
        return nextIndex;
    }

    @Override
    protected String doToString() {
        return  ", term: " + term + ", next-index: " + nextIndex + super.doToString();
    }
}
