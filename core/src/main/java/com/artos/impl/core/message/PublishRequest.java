/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

import com.artos.spi.core.LogEntry;
import com.exametrika.common.utils.Immutables;

import java.util.List;
import java.util.Set;
import java.util.UUID;

public class PublishRequest extends ClientRequest {
    private final List<LogEntry> logEntries;

    public PublishRequest(UUID groupId, UUID source, Set<MessageFlags> flags,
                          List<LogEntry> logEntries) {
        super(groupId, source, flags);

        this.logEntries = Immutables.wrap(logEntries);
    }

    public List<LogEntry> getLogEntries() {
        return logEntries;
    }

    @Override
    protected String doToString() {
        return  (logEntries != null ? ", entries: " + logEntries.size() : "") + super.doToString();
    }
}
