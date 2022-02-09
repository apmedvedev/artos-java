/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

import com.artos.impl.core.server.state.StateTransferConfiguration;

import java.util.Set;
import java.util.UUID;

public class JoinGroupResponse extends FollowerResponse {
    private final StateTransferConfiguration stateTransferConfiguration;

    public JoinGroupResponse(UUID groupId, UUID source, Set<MessageFlags> flags, boolean accepted,
                             UUID leaderId, StateTransferConfiguration stateTransferConfiguration) {
        super(groupId, source, flags, accepted, leaderId);

        this.stateTransferConfiguration = stateTransferConfiguration;
    }

    public StateTransferConfiguration getStateTransferConfiguration() {
        return stateTransferConfiguration;
    }

    @Override
    protected String doToString() {
        return  ", state-transfer-configuration: " + stateTransferConfiguration + super.doToString();
    }
}
