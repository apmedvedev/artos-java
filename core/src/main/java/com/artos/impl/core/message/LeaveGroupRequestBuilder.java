/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

public class LeaveGroupRequestBuilder extends FollowerRequestBuilder {

    public LeaveGroupRequest toMessage() {
        return new LeaveGroupRequest(getGroupId(), getSource(), getFlags());
    }
}
