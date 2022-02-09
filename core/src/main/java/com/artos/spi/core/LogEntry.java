/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.spi.core;

import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.ByteArray;

import java.util.UUID;

public class LogEntry {
    public static final LogEntry NULL_ENTRY = new LogEntry();

    private final long term;
    private final ByteArray value;
    private final LogValueType vaueType;
    private final UUID clientId;
    private final long messageId;

    public LogEntry(long term, ByteArray value, LogValueType valueType, UUID clientId, long messageId) {
        Assert.notNull(valueType);

        this.term = term;
        this.value = value;
        this.vaueType = valueType;
        this.clientId = clientId;
        this.messageId = messageId;
    }

    public long getTerm() {
        return this.term;
    }

    public ByteArray getValue() {
        return this.value;
    }

    public LogValueType getValueType() {
        return this.vaueType;
    }

    public UUID getClientId() {
        return clientId;
    }

    public long getMessageId() {
        return messageId;
    }

    @Override
    public String toString() {
        return "[term: " + term + ", type: " + vaueType + ", client-id: " + clientId + ", message-id: " + messageId + "]";
    }

    private LogEntry() {
        this(0, null, LogValueType.APPLICATION, null, 0);
    }
}
