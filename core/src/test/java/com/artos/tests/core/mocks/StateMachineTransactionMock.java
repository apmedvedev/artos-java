package com.artos.tests.core.mocks;

import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.spi.core.IStateMachineTransaction;
import com.artos.spi.core.ServerState;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.ICompletionHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class StateMachineTransactionMock implements IStateMachineTransaction {
    private boolean readOnly;
    private GroupConfiguration configuration;
    private ServerState serverState;
    private final List<ByteArray> commits = new ArrayList<>();
    private long lastCommitIndex;
    private boolean committed;
    private int commitCount;

    public GroupConfiguration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(GroupConfiguration configuration) {
        this.configuration = configuration;
    }

    public void init(boolean readOnly) {
        this.readOnly = readOnly;
        committed = false;
    }

    public void reset() {
        committed = false;
        commitCount = 0;
        lastCommitIndex = 0;
    }

    public long getLastCommitIndex() {
        return lastCommitIndex;
    }

    public void setLastCommitIndex(long lastCommitIndex) {
        this.lastCommitIndex = lastCommitIndex;
    }

    public List<ByteArray> getCommits() {
        return commits;
    }

    public boolean isCommitted() {
        return committed;
    }

    public int getCommitCount() {
        return commitCount;
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public GroupConfiguration readConfiguration() {
        assertThat(committed, is(false));
        return configuration;
    }

    @Override
    public void writeConfiguration(GroupConfiguration configuration) {
        assertThat(committed, is(false));
        assertThat(readOnly, is(false));
        this.configuration = configuration;
    }

    @Override
    public ServerState readState() {
        assertThat(committed, is(false));
        return serverState;
    }

    @Override
    public void writeState(ServerState serverState) {
        assertThat(committed, is(false));
        assertThat(readOnly, is(false));
        this.serverState = serverState;
    }

    @Override
    public void publish(long logIndex, ByteArray data) {
        assertThat(committed, is(false));
        assertThat(readOnly, is(false));
        assertThat(logIndex, is(lastCommitIndex + 1));
        commits.add(data);
        lastCommitIndex = logIndex;
    }

    @Override
    public void query(ByteArray request, ICompletionHandler<ByteArray> responseHandler) {
    }

    @Override
    public void subscribe(UUID subscriptionId, ByteArray request, ICompletionHandler<ByteArray> responseHandler) {
    }

    @Override
    public void unsubscribe(UUID subscriptionId) {
    }

    @Override
    public void commit() {
        committed = true;
        commitCount++;
    }
}
