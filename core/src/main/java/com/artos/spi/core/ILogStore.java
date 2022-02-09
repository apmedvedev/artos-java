/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.spi.core;

import com.exametrika.common.utils.ILifecycle;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public interface ILogStore extends ILifecycle {

    /**
     * The start index of the log store, at the very beginning, it must be 1
     * however, after some compact actions, this could be anything greater or equals to one
     * @return start index of the log store
     */
    long getStartIndex();

    /**
     * The end index of the store (exclusive), which is next index after index of last entry. Starts with 1
     * @return value &gt;= 1
     */
    long getEndIndex();

    /**
     * The last log entry in store
     * @return a dummy constant entry with value set to null and term set to zero if no log entry in store
     */
    LogEntry getLast();

    /**
     * Get log entries with index between {@code start} and {@code end}
     * @param startLogIndex the start index of log entries
     * @param endLogIndex the end index of log entries (exclusive)
     * @return the log entries between [start, end)
     */
    List<LogEntry> get(long startLogIndex, long endLogIndex);

    /**
     * Gets the log entry at the specified index
     * @param logIndex starts from 1
     * @return the log entry or null if index &gt;= {@code this.getEndIndex()}
     */
    LogEntry getAt(long logIndex);

    /**
     * Returns commit index.
     *
     * @return commit index
     */
    long getCommitIndex();

    /**
     * Appends a log entry to store
     * @param logEntry log entry to append
     * @return the last appended log index
     */
    long append(LogEntry logEntry);

    /**
     * Over writes a log entry at index of {@code index}
     * @param logIndex a value &lt; {@code this.getEndIndex()}, and starts from 1
     * @param logEntry log entry to write
     */
    void setAt(long logIndex, LogEntry logEntry);

    /**
     * Clears log store and set its start index to given value.
     *
     * @param startLogIndex start index of log store
     */
    void clear(long startLogIndex);

    /**
     * Commit log entries up to given log index.
     *
     * @param logIndex commit log index
     */
    void commit(long logIndex);

    /**
     * Reads log entries starting from given log index by writing them to given stream.
     *
     * @param startLogIndex - log index of start log entry
     * @param stream stream to write log entries to
     * @param maxSize maximum size in bytes to be written in given stream
     * @return index of next log entry to be written or -1 if last log entry has been written
     */
    long read(long startLogIndex, OutputStream stream, long maxSize);

    /**
     * Writes given log entries to log store by reading them from given stream.
     *
     * @param stream stream of log entries data
     */
    void write(InputStream stream);

    /**
     * Is compaction locked?
     *
     * @return true if compaction is locked
     */
    boolean isCompactionLocked();

    /**
     * Locks log compaction.
     */
    void lockCompaction();

    /**
     * Unlocks log compaction.
     */
    void unlockCompaction();
}
