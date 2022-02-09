/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

import com.artos.spi.core.LogEntry;

import java.util.List;

public class PublishRequestBuilder extends ClientRequestBuilder {
    private List<LogEntry> logEntries;

    public List<LogEntry> getLogEntries() {
        return logEntries;
    }

    public void setLogEntries(List<LogEntry> logEntries) {
        this.logEntries = logEntries;
    }

    public PublishRequest toMessage() {
        return new PublishRequest(getGroupId(), getSource(), getFlags(), logEntries);
    }
}
