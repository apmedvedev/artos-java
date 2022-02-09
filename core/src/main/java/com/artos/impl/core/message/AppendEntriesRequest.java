/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

import com.artos.spi.core.LogEntry;
import com.exametrika.common.utils.Immutables;

import java.util.List;
import java.util.Set;
import java.util.UUID;

public class AppendEntriesRequest extends ServerRequest {
    private final long term;
    private final long lastLogTerm;
    private final long lastLogIndex;
    private final long commitIndex;
    private final List<LogEntry> logEntries;

    public AppendEntriesRequest(UUID groupId, UUID source, Set<MessageFlags> flags,
                                long term, long lastLogTerm, long lastLogIndex, long commitIndex, List<LogEntry> logEntries) {
        super(groupId, source, flags);

        this.term = term;
        this.lastLogTerm = lastLogTerm;
        this.lastLogIndex = lastLogIndex;
        this.commitIndex = commitIndex;
        this.logEntries = Immutables.wrap(logEntries);
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

    public long getCommitIndex() {
        return commitIndex;
    }

    public List<LogEntry> getLogEntries() {
        return logEntries;
    }

    @Override
    protected String doToString() {
        return  ", term: " + term + ", last-log-term: " + lastLogTerm + ", last-log-index: " + lastLogIndex + ", commit-index: " + commitIndex +
            (logEntries != null ? ", entries: " + logEntries.size() : "") + super.doToString();
    }
}
