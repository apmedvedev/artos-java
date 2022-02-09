/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

import java.util.Set;
import java.util.UUID;

public class LeaveGroupRequest extends FollowerRequest {

    public LeaveGroupRequest(UUID groupId, UUID source, Set<MessageFlags> flags) {
        super(groupId, source, flags);
    }
}
