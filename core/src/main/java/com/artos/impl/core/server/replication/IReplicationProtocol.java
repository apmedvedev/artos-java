package com.artos.impl.core.server.replication;

import com.artos.impl.core.message.AppendEntriesRequest;
import com.artos.impl.core.message.AppendEntriesResponse;
import com.artos.impl.core.message.GroupTransientState;
import com.artos.impl.core.message.GroupTransientStateBuilder;
import com.artos.impl.core.message.PublishRequest;
import com.artos.impl.core.message.PublishResponse;
import com.artos.spi.core.LogEntry;
import com.exametrika.common.compartment.ICompartmentTimerProcessor;
import com.exametrika.common.utils.ILifecycle;

import java.util.Collection;

public interface IReplicationProtocol extends ILifecycle, ICompartmentTimerProcessor {
    ClientSessionManager getClientSessionManager();

    void onLeader();

    void onFollower();

    void onLeft();

    long publish(LogEntry logEntry);

    long getLastReceivedMessageId();

    void requestAppendEntries();

    void lockCommits();

    void unlockCommits();

    void acquireTransientState(GroupTransientStateBuilder transientState);

    void applyTransientState(Collection<GroupTransientState> transientStates);

    AppendEntriesResponse handleAppendEntriesRequest(AppendEntriesRequest request);

    PublishResponse handlePublishRequest(PublishRequest request);

    void handleAppendEntriesResponse(AppendEntriesResponse response);

    void onSessionRemoved(ClientSession session);
}
