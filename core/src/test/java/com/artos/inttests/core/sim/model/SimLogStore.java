package com.artos.inttests.core.sim.model;

import com.artos.inttests.core.sim.perf.IMeter;
import com.artos.inttests.core.sim.perf.PerfRegistry;
import com.artos.spi.core.ILogStore;
import com.artos.spi.core.LogEntry;
import com.exametrika.common.utils.Assert;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public class SimLogStore implements ILogStore {
    private final ILogStore logStore;
    private final IMeter uncommittedEntriesMeter;
    private final IMeter currentEntriesMeter;
    private final IMeter totalEntriesMeter;
    private final IMeter commitsMeter;
    private final IMeter readsMeter;
    private final IMeter writesMeter;

    public SimLogStore(String endpoint, ILogStore logStore, PerfRegistry perfRegistry) {
        Assert.notNull(endpoint);
        Assert.notNull(logStore);
        Assert.notNull(perfRegistry);

        this.logStore = logStore;
        currentEntriesMeter = perfRegistry.getMeter("log-store:" + endpoint + ".entries.current(counts)");
        uncommittedEntriesMeter = perfRegistry.getMeter("log-store:" + endpoint + ".entries.uncommitted(counts)");
        totalEntriesMeter = perfRegistry.getMeter("log-store:" + endpoint + ".entries.total(bytes)");
        commitsMeter = perfRegistry.getMeter("log-store:" + endpoint + ".commits(counts)");
        readsMeter = perfRegistry.getMeter("log-store:" + endpoint + ".reads(counts)");
        writesMeter = perfRegistry.getMeter("log-store:" + endpoint + ".writes(counts)");
    }
    @Override
    public long getStartIndex() {
        return logStore.getStartIndex();
    }

    @Override
    public long getEndIndex() {
        return logStore.getEndIndex();
    }

    @Override
    public LogEntry getLast() {
        return logStore.getLast();
    }

    @Override
    public List<LogEntry> get(long startLogIndex, long endLogIndex) {
        return logStore.get(startLogIndex, endLogIndex);
    }

    @Override
    public LogEntry getAt(long logIndex) {
        return logStore.getAt(logIndex);
    }

    @Override
    public long getCommitIndex() {
        return logStore.getCommitIndex();
    }

    @Override
    public long append(LogEntry logEntry) {
        totalEntriesMeter.measure(logEntry.getValue().getLength());
        currentEntriesMeter.measure(logStore.getEndIndex() - logStore.getStartIndex());
        uncommittedEntriesMeter.measure(logStore.getEndIndex() - logStore.getCommitIndex());
        return logStore.append(logEntry);
    }

    @Override
    public void setAt(long logIndex, LogEntry logEntry) {
        logStore.setAt(logIndex, logEntry);
    }

    @Override
    public void clear(long startLogIndex) {
        logStore.clear(startLogIndex);
    }

    @Override
    public void commit(long logIndex) {
        commitsMeter.measure(1);

        logStore.commit(logIndex);
    }

    @Override
    public long read(long startLogIndex, OutputStream stream, long maxSize) {
        readsMeter.measure(1);

        return logStore.read(startLogIndex, stream, maxSize);
    }

    @Override
    public void write(InputStream stream) {
        writesMeter.measure(1);

        logStore.write(stream);
    }

    @Override
    public boolean isCompactionLocked() {
        return logStore.isCompactionLocked();
    }

    @Override
    public void lockCompaction() {
        logStore.lockCompaction();
    }

    @Override
    public void unlockCompaction() {
        logStore.unlockCompaction();
    }

    @Override
    public void start() {
        logStore.start();
    }

    @Override
    public void stop() {
        logStore.stop();
    }
}
