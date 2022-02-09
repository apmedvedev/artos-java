package com.artos.impl.core.message;

import java.util.List;

public class GroupTransientStateBuilder {
    private List<ClientSessionTransientState> clientSessions;
    private long commitIndex;

    public List<ClientSessionTransientState> getClientSessions() {
        return clientSessions;
    }

    public void setClientSessions(List<ClientSessionTransientState> clientSessions) {
        this.clientSessions = clientSessions;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public GroupTransientState toState() {
        return new GroupTransientState(clientSessions, commitIndex);
    }
}
