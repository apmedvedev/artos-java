package com.artos.tests.core.mocks;

import com.artos.api.core.server.conf.ServerConfiguration;
import com.artos.impl.core.message.ServerRequest;
import com.artos.impl.core.server.impl.IServerPeer;

public class ServerPeerMock implements IServerPeer {
    private ServerConfiguration configuration;
    private boolean heartbeatEnabled;
    private long nextLogIndex;
    private long matchedIndex;
    private long rewindStep = 1;
    private long lastResponseTime;

    public void setConfiguration(ServerConfiguration configuration) {
        this.configuration = configuration;
    }

    public boolean isHeartbeatEnabled() {
        return heartbeatEnabled;
    }

    public void setLastResponseTime(long lastResponseTime) {
        this.lastResponseTime = lastResponseTime;
    }

    @Override
    public ServerConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public boolean isConnected() {
        return true;
    }

    @Override
    public boolean isRequestInProgress() {
        return false;
    }

    @Override
    public boolean canHeartbeat(long currentTime) {
        return heartbeatEnabled;
    }

    @Override
    public long getLastResponseTime() {
        return lastResponseTime;
    }

    @Override
    public long getNextLogIndex() {
        return nextLogIndex;
    }

    @Override
    public void setNextLogIndex(long nextLogIndex) {
        this.nextLogIndex = nextLogIndex;
    }

    @Override
    public long getRewindStep() {
        return rewindStep;
    }

    @Override
    public void setRewindStep(long value) {
        this.rewindStep = value;
    }

    @Override
    public long getMatchedIndex() {
        return matchedIndex;
    }

    @Override
    public void setMatchedIndex(long matchedIndex) {
        this.matchedIndex = matchedIndex;
    }

    @Override
    public void send(ServerRequest request) {
    }

    @Override
    public void start() {
        heartbeatEnabled = true;
    }

    @Override
    public void stop() {
        heartbeatEnabled = false;
    }
}
