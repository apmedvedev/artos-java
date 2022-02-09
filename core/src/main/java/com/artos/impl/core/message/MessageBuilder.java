/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

import com.exametrika.common.utils.Enums;

import java.util.Set;
import java.util.UUID;

public class MessageBuilder {
    private UUID groupId;
    private UUID source;
    private Set<MessageFlags> flags = Enums.noneOf(MessageFlags.class);

    public UUID getGroupId() {
        return groupId;
    }

    public void setGroupId(UUID groupId) {
        this.groupId = groupId;
    }

    public UUID getSource() {
        return source;
    }

    public void setSource(UUID source) {
        this.source = source;
    }

    public Set<MessageFlags> getFlags() {
        return flags;
    }

    public void setFlags(Set<MessageFlags> flags) {
        this.flags = flags;
    }
}
