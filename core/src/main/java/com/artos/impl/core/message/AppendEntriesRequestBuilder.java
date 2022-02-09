/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

import com.artos.spi.core.LogEntry;

import java.util.List;

public class AppendEntriesRequestBuilder extends ServerRequestBuilder {
    private long term;
    private long lastLogTerm;
    private long lastLogIndex;
    private long commitIndex;
    private List<LogEntry> logEntries;

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

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public List<LogEntry> getLogEntries() {
        return logEntries;
    }

    public void setLogEntries(List<LogEntry> logEntries) {
        this.logEntries = logEntries;
    }

    public AppendEntriesRequest toMessage() {
        return new AppendEntriesRequest(getGroupId(), getSource(), getFlags(),
            term, lastLogTerm, lastLogIndex, commitIndex, logEntries);
    }
}
