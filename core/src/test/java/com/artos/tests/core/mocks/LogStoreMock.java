package com.artos.tests.core.mocks;

import com.artos.impl.core.server.log.MemoryLogStore;
import com.artos.spi.core.LogEntry;

import java.util.List;

public class LogStoreMock extends MemoryLogStore {
    private boolean compactionLocked;
    private boolean compactionUnlocked;

    public LogStoreMock(long minRetentionPeriod) {
        super(minRetentionPeriod);
    }

    public void setStartIndex(long startIndex) {
        this.startIndex = startIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public List<LogEntry> getEntries() {
        return get(startIndex, getEndIndex());
    }

    @Override
    public boolean isCompactionLocked() {
        return compactionLocked;
    }

    public boolean isCompactionUnlocked() {
        return compactionUnlocked;
    }

    @Override
    public void lockCompaction() {
        compactionLocked = true;
    }

    @Override
    public void unlockCompaction() {
        compactionUnlocked = true;
    }
}
