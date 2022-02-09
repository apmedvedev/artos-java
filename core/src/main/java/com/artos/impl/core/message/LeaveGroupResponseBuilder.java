/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

import com.artos.api.core.server.conf.GroupConfiguration;

public class LeaveGroupResponseBuilder extends FollowerResponseBuilder {
    private GroupConfiguration configuration;

    public LeaveGroupResponse toMessage() {
        return new LeaveGroupResponse(getGroupId(), getSource(), getFlags(), isAccepted(), getLeaderId(), configuration);
    }

    public LeaveGroupResponseBuilder setConfiguration(GroupConfiguration configuration) {
        this.configuration = configuration;

        return this;
    }
}
