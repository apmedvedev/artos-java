package com.artos.tests.core.mocks;

import com.artos.impl.core.message.AppendEntriesRequest;
import com.artos.impl.core.message.AppendEntriesResponse;
import com.artos.impl.core.message.GroupTransientState;
import com.artos.impl.core.message.GroupTransientStateBuilder;
import com.artos.impl.core.message.PublishRequest;
import com.artos.impl.core.message.PublishResponse;
import com.artos.impl.core.server.replication.ClientSession;
import com.artos.impl.core.server.replication.ClientSessionManager;
import com.artos.impl.core.server.replication.IReplicationProtocol;
import com.artos.spi.core.LogEntry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ReplicationProtocolMock implements IReplicationProtocol {
    private boolean onLeader;
    private List<LogEntry> logEntries = new ArrayList<>();
    private long lastCommittedMessageId;
    private boolean onFollower;
    private boolean appendEntriesRequested;
    private boolean commitsLocked;
    private boolean commitsUnlocked;

    public boolean isOnLeader() {
        return onLeader;
    }

    public boolean isOnFollower() {
        return onFollower;
    }

    public boolean isAppendEntriesRequested() {
        return appendEntriesRequested;
    }

    public boolean isCommitsLocked() {
        return commitsLocked;
    }

    public boolean isCommitsUnlocked() {
        return commitsUnlocked;
    }

    public void setLastCommittedMessageId(long lastCommittedMessageId) {
        this.lastCommittedMessageId = lastCommittedMessageId;
    }

    public List<LogEntry> getLogEntries() {
        return logEntries;
    }

    @Override
    public ClientSessionManager getClientSessionManager() {
        return null;
    }

    @Override
    public void onLeader() {
        onLeader = true;
    }

    @Override
    public void onFollower() {
        onFollower = true;
    }

    @Override
    public void onLeft() {
    }

    @Override
    public long publish(LogEntry logEntry) {
        this.logEntries.addAll(logEntries);
        return lastCommittedMessageId;
    }

    @Override
    public long getLastReceivedMessageId() {
        return 0;
    }

    @Override
    public void requestAppendEntries() {
        appendEntriesRequested = true;
    }

    @Override
    public void lockCommits() {
        commitsLocked = true;
    }

    @Override
    public void unlockCommits() {
        commitsUnlocked = true;
    }

    @Override
    public void acquireTransientState(GroupTransientStateBuilder transientState) {
    }

    @Override
    public void applyTransientState(Collection<GroupTransientState> transientStates) {
    }

    @Override
    public AppendEntriesResponse handleAppendEntriesRequest(AppendEntriesRequest request) {
        return null;
    }

    @Override
    public PublishResponse handlePublishRequest(PublishRequest request) {
        return null;
    }

    @Override
    public void handleAppendEntriesResponse(AppendEntriesResponse response) {
    }

    @Override
    public void onSessionRemoved(ClientSession session) {
    }

    @Override
    public void onTimer(long currentTime) {
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }
}
