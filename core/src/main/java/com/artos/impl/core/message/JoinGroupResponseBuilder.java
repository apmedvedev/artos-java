/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

import com.artos.impl.core.server.state.StateTransferConfiguration;

public class JoinGroupResponseBuilder extends FollowerResponseBuilder {
    private StateTransferConfiguration stateTransferConfiguration;

    public StateTransferConfiguration getStateTransferConfiguration() {
        return stateTransferConfiguration;
    }

    public void setStateTransferConfiguration(StateTransferConfiguration stateTransferConfiguration) {
        this.stateTransferConfiguration = stateTransferConfiguration;
    }

    public JoinGroupResponse toMessage() {
        return new JoinGroupResponse(getGroupId(), getSource(), getFlags(), isAccepted(), getLeaderId(), stateTransferConfiguration);
    }
}
