package com.artos.impl.core.server.log;

import com.artos.impl.core.server.utils.Utils;
import com.artos.spi.core.ILogStore;
import com.artos.spi.core.LogEntry;
import com.exametrika.common.io.impl.ByteInputStream;
import com.exametrika.common.io.impl.ByteOutputStream;
import com.exametrika.common.io.impl.DataDeserialization;
import com.exametrika.common.io.impl.DataSerialization;
import com.exametrika.common.l10n.DefaultMessage;
import com.exametrika.common.l10n.ILocalizedMessage;
import com.exametrika.common.l10n.Messages;
import com.exametrika.common.log.ILogger;
import com.exametrika.common.log.LogLevel;
import com.exametrika.common.log.Loggers;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.Exceptions;
import com.exametrika.common.utils.IOs;
import com.exametrika.common.utils.SimpleDeque;
import com.exametrika.common.utils.Times;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class MemoryLogStore implements ILogStore {
    private static final IMessages messages = Messages.get(IMessages.class);
    private final ILogger logger = Loggers.get(getClass());
    private final long minRetentionPeriod;
    protected long startIndex = 1;
    private SimpleDeque<EntryInfo> entries = new SimpleDeque<>();
    protected long commitIndex;
    private boolean compactionLocked;

    public MemoryLogStore(long minRetentionPeriod) {
        this.minRetentionPeriod = minRetentionPeriod;
    }

    @Override
    public synchronized long getEndIndex() {
        return startIndex + entries.size();
    }

    @Override
    public synchronized long getStartIndex() {
        return startIndex;
    }

    @Override
    public synchronized LogEntry getLast() {
        if (!entries.isEmpty())
            return entries.get(entries.size() - 1).logEntry;
        else
            return LogEntry.NULL_ENTRY;
    }

    @Override
    public synchronized List<LogEntry> get(long startLogIndex, long endLogIndex) {
        Assert.isTrue(startLogIndex <= endLogIndex && startLogIndex >= startIndex && endLogIndex <= startIndex + entries.size());

        int startIndex = (int) (startLogIndex - this.startIndex);
        int endIndex = (int) (endLogIndex - this.startIndex);

        List<LogEntry> logEntries = new ArrayList<>();
        for (int i = startIndex; i < endIndex; i++)
            logEntries.add(entries.get(i).logEntry);

        return logEntries;
    }

    @Override
    public synchronized LogEntry getAt(long logIndex) {
        Assert.isTrue(logIndex >= startIndex && logIndex < startIndex + entries.size());

        int i = (int) (logIndex - this.startIndex);
        return entries.get(i).logEntry;
    }

    @Override
    public synchronized long getCommitIndex() {
        return commitIndex;
    }

    @Override
    public synchronized long append(LogEntry logEntry) {
        entries.addLast(new EntryInfo(logEntry, Times.getCurrentTime()));
        return startIndex + entries.size() - 1;
    }

    @Override
    public synchronized void setAt(long logIndex, LogEntry logEntry) {
        Assert.checkState(logIndex >= startIndex && logIndex <= startIndex + entries.size());

        trim(logIndex);
        entries.addLast(new EntryInfo(logEntry, Times.getCurrentTime()));
    }

    @Override
    public synchronized void clear(long startLogIndex) {
        entries.clear();
        this.startIndex = startLogIndex;
        this.commitIndex = startLogIndex;

        if (logger.isLogEnabled(LogLevel.DEBUG))
            logger.log(LogLevel.DEBUG, messages.logCleared(startLogIndex));
    }

    @Override
    public synchronized void commit(long logIndex) {
        Assert.checkState(logIndex >= startIndex && logIndex < startIndex + entries.size());

        this.commitIndex = logIndex;
        compact();
    }

    @Override
    public long read(long startLogIndex, OutputStream stream, long maxSize) {
        List<LogEntry> logEntries = new ArrayList<>();
        int logIndex;
        synchronized (this) {
            Assert.checkState(startLogIndex >= startIndex && startLogIndex <= startIndex + entries.size());

            int size = 0;
            logIndex = (int) (startLogIndex - startIndex);
            while (logIndex < entries.size()) {
                LogEntry logEntry = entries.get(logIndex).logEntry;

                if (logEntry.getValue() != null)
                    size += logEntry.getValue().getLength();
                else
                    size++;

                if (size > maxSize)
                    break;

                logEntries.add(logEntry);
                logIndex++;
            }

            if (logIndex >= entries.size())
                logIndex = -1;
            else
                logIndex += startIndex;
        }

        ByteOutputStream out = new ByteOutputStream();
        DataSerialization serialization = new DataSerialization(out);
        serialization.writeLong(startLogIndex);

        for (LogEntry logEntry : logEntries) {
            serialization.writeBoolean(true);
            Utils.writeLogEntry(serialization, logEntry);
        }

        serialization.writeBoolean(false);

        ByteInputStream in = new ByteInputStream(out.getBuffer(), 0, out.getLength());
        try {
            IOs.copy(in, stream);
        } catch (IOException e) {
            Exceptions.wrapAndThrow(e);
        }

        if (logger.isLogEnabled(LogLevel.DEBUG))
            logger.log(LogLevel.DEBUG, messages.logRead(startLogIndex, logEntries.size(), logIndex));

        return logIndex;
    }

    @Override
    public void write(InputStream stream) {
        ByteOutputStream out = new ByteOutputStream();
        try {
            IOs.copy(stream, out);
        } catch (IOException e) {
            Exceptions.wrapAndThrow(e);
        }

        DataDeserialization deserialization = new DataDeserialization(new ByteInputStream(out.getBuffer(), 0, out.getLength()));

        long startLogIndex = deserialization.readLong();
        List<LogEntry> logEntries = new ArrayList<>();
        while (deserialization.readBoolean()) {
            LogEntry logEntry = Utils.readLogEntry(deserialization);
            logEntries.add(logEntry);
        }

        synchronized (this) {
            Assert.checkState(startLogIndex >= startIndex);
            Assert.checkState(startLogIndex <= startIndex + entries.size());
            trim(startLogIndex);

            for (LogEntry logEntry : logEntries)
                entries.addLast(new EntryInfo(logEntry, Times.getCurrentTime()));
        }

        if (logger.isLogEnabled(LogLevel.DEBUG))
            logger.log(LogLevel.DEBUG, messages.logWritten(startLogIndex, logEntries.size()));
    }

    @Override
    public synchronized boolean isCompactionLocked() {
        return compactionLocked;
    }

    @Override
    public synchronized void lockCompaction() {
        compactionLocked = true;
    }

    @Override
    public synchronized void unlockCompaction() {
        compactionLocked = false;
        compact();
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    private void trim(long logIndex) {
        long removeCount = entries.size() - (logIndex - startIndex);
        for (int i = 0; i < removeCount; i++)
            entries.removeLast();
    }

    private void compact() {
        if (compactionLocked || entries.isEmpty())
            return;

        long currentTime = Times.getCurrentTime();
        while (commitIndex - startIndex > 0) {
            EntryInfo info = entries.getFirst();
            if (minRetentionPeriod == 0 || currentTime > info.creationTime + minRetentionPeriod) {
                entries.removeFirst();
                startIndex++;
            } else {
                break;
            }
        }
    }

    private static class EntryInfo {
        private final LogEntry logEntry;
        private final long creationTime;

        private EntryInfo(LogEntry logEntry, long creationTime) {
            this.logEntry = logEntry;
            this.creationTime = creationTime;
        }
    }

    private interface IMessages {
        @DefaultMessage("Log has been written from stream. Start log index: {0}, count: {1}")
        ILocalizedMessage logWritten(long startLogIndex, int count);

        @DefaultMessage("Log has been read to stream. Start log index: {0}, count: {1}, next log index: {2}")
        ILocalizedMessage logRead(long startLogIndex, int count, int nextLogIndex);

        @DefaultMessage("Log has been cleared. Start log index: {0}")
        ILocalizedMessage logCleared(long startLogIndex);
    }
}
