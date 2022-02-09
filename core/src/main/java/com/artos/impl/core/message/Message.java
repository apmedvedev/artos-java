/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.Immutables;

import java.util.Set;
import java.util.UUID;

public abstract class Message {
    private final UUID groupId;
    private final UUID source;
    private final Set<MessageFlags> flags;

    public Message(UUID groupId, UUID source, Set<MessageFlags> flags) {
        Assert.notNull(groupId);
        Assert.notNull(source);
        Assert.notNull(flags);

        this.groupId = groupId;
        this.source = source;
        this.flags = Immutables.wrap(flags);
    }

    public UUID getGroupId() {
        return groupId;
    }

    public UUID getSource() {
        return source;
    }

    public Set<MessageFlags> getFlags() {
        return flags;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[group-id: " + groupId + ", source: " + source +
            doToString() + ", flags: " + flags + "]";
    }

    public String toString(String source, boolean additionalFields) {
        return getClass().getSimpleName() + "[source: " + source +
            ", flags: " + flags + (additionalFields ? (", group-id: " + groupId + doToString()) : "") + "]";
    }

    protected abstract String doToString();
}
