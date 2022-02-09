package com.artos.impl.core.message;

import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.Immutables;

import java.util.List;

public class GroupTransientState {
    private final List<ClientSessionTransientState> clientSessions;
    private final long commitIndex;

    public GroupTransientState(List<ClientSessionTransientState> clientSessions, long commitIndex) {
        Assert.notNull(clientSessions);

        this.clientSessions = Immutables.wrap(clientSessions);
        this.commitIndex = commitIndex;
    }

    public List<ClientSessionTransientState> getClientSessions() {
        return clientSessions;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    @Override
    public String toString() {
        return "commit-index: " + commitIndex + ", client-sessions: " + clientSessions.toString();
    }
}
