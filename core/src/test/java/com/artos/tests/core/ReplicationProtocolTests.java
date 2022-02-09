package com.artos.tests.core;

import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.api.core.server.conf.ServerChannelFactoryConfigurationBuilder;
import com.artos.api.core.server.conf.ServerConfiguration;
import com.artos.impl.core.message.AppendEntriesRequest;
import com.artos.impl.core.message.AppendEntriesRequestBuilder;
import com.artos.impl.core.message.AppendEntriesResponse;
import com.artos.impl.core.message.AppendEntriesResponseBuilder;
import com.artos.impl.core.message.MessageFlags;
import com.artos.impl.core.message.PublishRequestBuilder;
import com.artos.impl.core.message.PublishResponse;
import com.artos.impl.core.message.ServerRequest;
import com.artos.impl.core.server.impl.IServerPeer;
import com.artos.impl.core.server.impl.ServerRole;
import com.artos.impl.core.server.replication.ClientSession;
import com.artos.impl.core.server.replication.ClientSessionManager;
import com.artos.impl.core.server.replication.ReplicationProtocol;
import com.artos.impl.core.server.state.StateTransferConfiguration;
import com.artos.impl.core.server.utils.Utils;
import com.artos.spi.core.LogEntry;
import com.artos.spi.core.LogValueType;
import com.artos.tests.core.mocks.ElectionProtocolMock;
import com.artos.tests.core.mocks.LogStoreMock;
import com.artos.tests.core.mocks.MembershipProtocolMock;
import com.artos.tests.core.mocks.ServerMock;
import com.artos.tests.core.mocks.ServerPeerMock;
import com.artos.tests.core.mocks.StateMachineMock;
import com.artos.tests.core.mocks.StateMachineTransactionMock;
import com.artos.tests.core.mocks.StateTransferProtocolMock;
import com.exametrika.common.tests.Expected;
import com.exametrika.common.tests.Tests;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.Enums;
import com.exametrika.common.utils.Pair;
import com.exametrika.common.utils.SimpleList;
import com.exametrika.common.utils.Times;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

public class ReplicationProtocolTests extends AbstractRaftTest {
    private ReplicationProtocol replicationProtocol;
    private ServerMock server;
    private ElectionProtocolMock electionProtocol;
    private MembershipProtocolMock membershipProtocol;
    private StateTransferProtocolMock stateTransferProtocol;
    private StateMachineMock stateMachine;

    @Before
    public void setUp() throws Throwable {
        Times.setTest(0);

        createState(10);
        context = createContext(state.getConfiguration());
        stateMachine = (StateMachineMock) context.getStateMachine();
        server = new ServerMock();

        ServerChannelFactoryConfigurationBuilder builder = new ServerChannelFactoryConfigurationBuilder();
        builder.setMaxPublishingLogEntryCount(10).setHeartbeatPeriod(1000).setMinStateTransferGapLogEntryCount(30)
            .setClientSessionTimeout(10000);
        Tests.set(context, "serverChannelFactoryConfiguration", builder.toConfiguration());

        electionProtocol = new ElectionProtocolMock();
        membershipProtocol = new MembershipProtocolMock();
        stateTransferProtocol = new StateTransferProtocolMock();
        stateTransferProtocol.setHost("testHost");

        replicationProtocol = new ReplicationProtocol(server, context, state);
        replicationProtocol.setElectionProtocol(electionProtocol);
        replicationProtocol.setMembershipProtocol(membershipProtocol);
        replicationProtocol.setStateTransferProtocol(stateTransferProtocol);
    }

    @Test
    public void testInitConfiguration() {
        state.getServerState().setCommitIndex(1);
        List<ServerConfiguration> servers = new ArrayList<>(state.getConfiguration().getServers());
        servers.remove(state.getConfiguration().findServer(context.getServerId()));
        GroupConfiguration configuration = new GroupConfiguration(state.getConfiguration().getName(),
            state.getConfiguration().getGroupId(), servers, false);
        context.getStateMachine().beginTransaction(false).writeConfiguration(configuration);
        state.setConfiguration(null);
        replicationProtocol.start();

        StateMachineTransactionMock transaction = ((StateMachineMock) context.getStateMachine()).getTransaction();
        assertThat(transaction.isCommitted(), is(true));

        assertThat(state.getConfiguration(), is(context.getStateMachine().beginTransaction(true).readConfiguration()));
        assertThat(state.isCatchingUp(), is(true));
    }

    @Test
    public void testPeerCreation() {
        state.getServerState().setCommitIndex(1);
        state.setConfiguration(null);
        replicationProtocol.start();

        assertThat(state.getPeers().size(), is(state.getConfiguration().getServers().size() - 1));
        for (int i = 1; i < state.getConfiguration().getServers().size(); i++) {
            ServerConfiguration serverConfiguration = state.getConfiguration().getServers().get(i);
            IServerPeer peer = state.getPeers().get(serverConfiguration.getId());
            assertThat(peer.getConfiguration().getId(), is(serverConfiguration.getId()));
            assertThat(peer.getConfiguration().getEndpoint(), is(serverConfiguration.getEndpoint()));
        }

        assertThat(state.getQuickCommitIndex(), is(state.getServerState().getCommitIndex()));
    }

    @Test
    public void testHeartbeats() {
        replicationProtocol.start();

        state.getServerState().setTerm(2);
        state.setRole(ServerRole.LEADER);
        for (int i = 1; i <= state.getConfiguration().getServers().size() / 2; i++) {
            ServerConfiguration serverConfiguration = state.getConfiguration().getServers().get(i);
            IServerPeer peer = state.getPeers().get(serverConfiguration.getId());
            peer.start();
        }

        Times.setTest(1000);
        replicationProtocol.onTimer(1000);

        assertThat(server.getRequests().size(), is(state.getConfiguration().getServers().size() / 2));
        for (Pair<IServerPeer, ServerRequest> pair : server.getRequests()) {
            IServerPeer peer = state.getPeers().get(pair.getKey().getConfiguration().getId());
            assertThat(peer == pair.getKey(), is(true));

            AppendEntriesRequest request = (AppendEntriesRequest) pair.getValue();
            assertThat(request.getSource(), is(context.getServerId()));
            assertThat(request.getGroupId(), is(context.getGroupId()));
            assertThat(request.getTerm(), is(2L));
        }
    }

    @Test
    public void testRequestAppendEntriesSingle() {
        state.setConfiguration(new GroupConfiguration("test", context.getGroupId(), Arrays.asList(
                new ServerConfiguration(context.getServerId(), "testEndpoint")), false));
        StateMachineTransactionMock transaction = (StateMachineTransactionMock) context.getStateMachine().beginTransaction(false);
        transaction.writeConfiguration(state.getConfiguration());
        replicationProtocol.start();

        context.getLogStore().append(new LogEntry(2, new ByteArray(new byte[0]), LogValueType.APPLICATION, null, 0));
        state.getServerState().setTerm(2);
        replicationProtocol.requestAppendEntries();

        assertThat(server.getRequests().isEmpty(), is(true));
        assertThat(state.getQuickCommitIndex(), is(1L));
        assertThat(state.getServerState().getCommitIndex(), is(1L));
        assertThat(transaction.getCommits().size(), is(1));
    }

    @Test
    public void testRequestAppendEntriesMulti() {
        replicationProtocol.start();
        startPeers();

        state.getServerState().setTerm(2);
        state.setRole(ServerRole.LEADER);
        replicationProtocol.requestAppendEntries();

        assertThat(server.getRequests().size(), is(state.getConfiguration().getServers().size() - 1));
        for (Pair<IServerPeer, ServerRequest> pair : server.getRequests()) {
            IServerPeer peer = state.getPeers().get(pair.getKey().getConfiguration().getId());
            assertThat(peer == pair.getKey(), is(true));

            AppendEntriesRequest request = (AppendEntriesRequest) pair.getValue();
            assertThat(request.getSource(), is(context.getServerId()));
            assertThat(request.getGroupId(), is(context.getGroupId()));
            assertThat(request.getTerm(), is(2L));
        }
    }

    @Test
    public void testCreateAppendEntriesRequest() throws Exception {
        replicationProtocol.start();

        state.getServerState().setTerm(2);

        state.setRole(ServerRole.LEADER);
        IServerPeer peer = state.getPeers().get(state.getConfiguration().getServers().get(1).getId());
        AppendEntriesRequest request = Tests.invoke(replicationProtocol, "createAppendEntriesRequest", peer);

        assertThat(request.getGroupId(), is(context.getGroupId()));
        assertThat(request.getSource(), is(context.getServerId()));
        assertThat(request.getLastLogIndex(), is(0L));
        assertThat(request.getLastLogTerm(), is(0L));
        assertThat(request.getLogEntries(), nullValue());
        assertThat(request.getCommitIndex(), is(0L));
        assertThat(request.getTerm(), is(2L));
        assertThat(request.getFlags().isEmpty(), is(true));

        UUID clientId = UUID.randomUUID();
        addLogEntries(clientId, 2, 10);
        request = Tests.invoke(replicationProtocol, "createAppendEntriesRequest", peer);

        assertThat(request.getLastLogIndex(), is(0L));
        assertThat(request.getLastLogTerm(), is(0L));
        checkLogEntries(request.getLogEntries(), clientId, 2, 1, 10, true);
        assertThat(request.getCommitIndex(), is(0L));
        assertThat(request.getTerm(), is(2L));
        assertThat(request.getFlags().isEmpty(), is(true));

        peer.setNextLogIndex(11);
        state.setQuickCommitIndex(3);
        addLogEntries(clientId, 2, 20);
        request = Tests.invoke(replicationProtocol, "createAppendEntriesRequest", peer);

        assertThat(request.getLastLogIndex(), is(10L));
        assertThat(request.getLastLogTerm(), is(2L));
        checkLogEntries(request.getLogEntries(), clientId, 2, 11, 10, true);
        assertThat(request.getCommitIndex(), is(3L));
        assertThat(request.getTerm(), is(2L));
        assertThat(request.getFlags().isEmpty(), is(true));
    }

    @Test
    public void testCreateAppendEntriesRequestForOutdatedFollower() throws Exception {
        replicationProtocol.start();

        state.getServerState().setTerm(2);

        IServerPeer peer = state.getPeers().get(state.getConfiguration().getServers().get(1).getId());

        UUID clientId = UUID.randomUUID();
        addLogEntries(clientId, 2, 30);

        state.setRole(ServerRole.LEADER);
        stateTransferProtocol.setHost("testHost");
        stateTransferProtocol.setPort(1234);
        stateTransferProtocol.setStartLogIndex(5678);
        AppendEntriesRequest request = Tests.invoke(replicationProtocol, "createAppendEntriesRequest", peer);

        assertThat(request.getGroupId(), is(context.getGroupId()));
        assertThat(request.getSource(), is(context.getServerId()));
        assertThat(request.getLastLogIndex(), is(0L));
        assertThat(request.getLastLogTerm(), is(0L));
        assertThat(request.getCommitIndex(), is(0L));
        assertThat(request.getTerm(), is(2L));
        assertThat(request.getFlags(), is(Enums.of(MessageFlags.STATE_TRANSFER_REQUIRED, MessageFlags.TRANSFER_LOG)));
        assertThat(request.getLogEntries().size(), is(1));
        StateTransferConfiguration configuration = Utils.readStateTransferConfiguration(request.getLogEntries().get(0).getValue());
        assertThat(configuration.getHost(), is("testHost"));
        assertThat(configuration.getPort(), is(1234));
        assertThat(configuration.getStartLogIndex(), is(1L));

        LogStoreMock logStore = (LogStoreMock) context.getLogStore();
        logStore.setStartIndex(30);
        peer.setNextLogIndex(30);
        request = Tests.invoke(replicationProtocol, "createAppendEntriesRequest", peer);

        assertThat(request.getLastLogIndex(), is(29L));
        assertThat(request.getLastLogTerm(), is(-1L));
        assertThat(request.getCommitIndex(), is(0L));
        assertThat(request.getTerm(), is(2L));
        assertThat(request.getFlags(), is(Enums.of(MessageFlags.STATE_TRANSFER_REQUIRED, MessageFlags.TRANSFER_SNAPSHOT)));
        assertThat(request.getLogEntries().size(), is(1));
        configuration = Utils.readStateTransferConfiguration(request.getLogEntries().get(0).getValue());
        assertThat(configuration.getHost(), is("testHost"));
        assertThat(configuration.getPort(), is(1234));
        assertThat(configuration.getStartLogIndex(), is(1L));

        peer.setNextLogIndex(31);
        logStore.clear(logStore.getStartIndex());
        addLogEntries(clientId, 2, 10);

        request = Tests.invoke(replicationProtocol, "createAppendEntriesRequest", peer);

        assertThat(request.getLastLogIndex(), is(30L));
        assertThat(request.getLastLogTerm(), is(2L));
        checkLogEntries(request.getLogEntries(), clientId, 2, 31, 9, true);
        assertThat(request.getCommitIndex(), is(0L));
        assertThat(request.getTerm(), is(2L));
        assertThat(request.getFlags().isEmpty(), is(true));
    }

    @Test
    public void testAppendEntries() throws Exception {
        replicationProtocol.start();
        startPeers();

        state.getServerState().setTerm(2);
        state.setLeader(context.getServerId());

        List<LogEntry> logEntries = createLogEntries(context.getServerId(), 2, 1, 10);
        for (LogEntry logEntry : logEntries)
            assertThat(replicationProtocol.publish(logEntry), is(0L));
        LogStoreMock logStore = (LogStoreMock) context.getLogStore();
        assertThat(logStore.getEntries().isEmpty(), is(true));

        state.setRole(ServerRole.LEADER);

        for (LogEntry logEntry : logEntries)
            assertThat(replicationProtocol.publish(logEntry), is(0L));

        assertThat(server.getRequests().size(), is(state.getConfiguration().getServers().size() - 1));
        for (Pair<IServerPeer, ServerRequest> pair : server.getRequests()) {
            IServerPeer peer = state.getPeers().get(pair.getKey().getConfiguration().getId());
            assertThat(peer == pair.getKey(), is(true));

            AppendEntriesRequest serverRequest = (AppendEntriesRequest) pair.getValue();
            assertThat(serverRequest.getSource(), is(context.getServerId()));
            assertThat(serverRequest.getGroupId(), is(context.getGroupId()));
            assertThat(serverRequest.getTerm(), is(2L));
            checkLogEntries(serverRequest.getLogEntries(), context.getServerId(), 2, 1, 10, true);
        }

        server.getRequests().clear();

        checkLogEntries(logStore.getEntries(), context.getServerId(), 2, 1, 10, true);

        Times.setTest(1000);

        stateTransferProtocol.setHost("testHost");
        stateTransferProtocol.setPort(1234);
        stateTransferProtocol.setStartLogIndex(5678);
        Tests.invoke(replicationProtocol, "commit", 10);
        logEntries = createLogEntries(context.getServerId(), 2, 11, 20);

        for (int i = 0; i < logEntries.size(); i++) {
            LogEntry logEntry = logEntries.get(i);
            assertThat(replicationProtocol.publish(logEntry), is((long)(i + 1)));
        }
        checkLogEntries(logStore.getEntries(), context.getServerId(), 2, 10, 21, true);
    }

    @Test
    public void testHandlePublishRequest() throws Exception {
        replicationProtocol.start();
        startPeers();

        state.getServerState().setTerm(2);
        state.setLeader(context.getServerId());

        PublishRequestBuilder request = createPublishRequest();
        PublishResponse response = replicationProtocol.handlePublishRequest(request.toMessage());
        assertThat(response.isAccepted(), is(false));
        assertThat(response.getGroupId(), is(context.getGroupId()));
        assertThat(response.getSource(), is(context.getServerId()));
        assertThat(response.getLeaderId(), is(state.getLeader()));
        assertThat(response.getConfiguration() == state.getConfiguration(), is(true));

        state.setRole(ServerRole.LEADER);

        request.setFlags(Enums.of(MessageFlags.REQUEST_CONFIGURATION));
        response = replicationProtocol.handlePublishRequest(request.toMessage());
        assertThat(response.isAccepted(), is(true));
        assertThat(response.getConfiguration() == state.getConfiguration(), is(true));

        request.setFlags(Enums.noneOf(MessageFlags.class));
        request.setLogEntries(createLogEntries(UUID.randomUUID(), 0, 1, 10));
        response = replicationProtocol.handlePublishRequest(request.toMessage());
        assertThat(response.isAccepted(), is(true));
        assertThat(response.getConfiguration(), nullValue());
        assertThat(response.getLastReceivedMessageId(), is(10L));
        assertThat(response.getLastCommittedMessageId(), is(0L));
        assertThat(response.getFlags().isEmpty(), is(true));

        assertThat(server.getRequests().size(), is(state.getConfiguration().getServers().size() - 1));
        for (Pair<IServerPeer, ServerRequest> pair : server.getRequests()) {
            IServerPeer peer = state.getPeers().get(pair.getKey().getConfiguration().getId());
            assertThat(peer == pair.getKey(), is(true));

            AppendEntriesRequest serverRequest = (AppendEntriesRequest) pair.getValue();
            assertThat(serverRequest.getSource(), is(context.getServerId()));
            assertThat(serverRequest.getGroupId(), is(context.getGroupId()));
            assertThat(serverRequest.getTerm(), is(2L));
            checkLogEntries(serverRequest.getLogEntries(), request.getSource(), 2, 1, 10, true);
        }

        server.getRequests().clear();

        LogStoreMock logStore = (LogStoreMock) context.getLogStore();
        checkLogEntries(logStore.getEntries(), request.getSource(), 2, 1, 10, true);

        request.setLogEntries(createLogEntries(UUID.randomUUID(), 0, 11, 10));
        response = replicationProtocol.handlePublishRequest(request.toMessage());
        assertThat(response.isAccepted(), is(true));
        assertThat(response.getLastReceivedMessageId(), is(20L));
        assertThat(response.getFlags().isEmpty(), is(true));
        checkLogEntries(logStore.getEntries(), request.getSource(), 2, 1, 20, true);
        assertThat(server.getRequests().size(), is(state.getConfiguration().getServers().size() - 1));
        server.getRequests().clear();

        request.setLogEntries(Collections.singletonList(new LogEntry(0, new ByteArray(new byte[0]), LogValueType.APPLICATION,
            UUID.randomUUID(), 10)));
        response = replicationProtocol.handlePublishRequest(request.toMessage());
        assertThat(response.isAccepted(), is(true));
        assertThat(response.getLastReceivedMessageId(), is(20L));
        assertThat(response.getFlags().isEmpty(), is(true));
        checkLogEntries(logStore.getEntries(), request.getSource(), 2, 1, 20, true);
        assertThat(server.getRequests().isEmpty(), is(true));

        request.setLogEntries(Collections.singletonList(new LogEntry(0, new ByteArray(new byte[0]), LogValueType.APPLICATION,
            UUID.randomUUID(), 30)));
        response = replicationProtocol.handlePublishRequest(request.toMessage());
        assertThat(response.isAccepted(), is(true));
        assertThat(response.getLastReceivedMessageId(), is(20L));
        assertThat(response.getFlags(), is(Enums.of(MessageFlags.OUT_OF_ORDER_RECEIVED)));
        checkLogEntries(logStore.getEntries(), request.getSource(), 2, 1, 20, true);
        assertThat(server.getRequests().isEmpty(), is(true));

        Times.setTest(10000);
        replicationProtocol.onTimer(10000);

        request.setLogEntries(Collections.singletonList(new LogEntry(0, new ByteArray(new byte[0]), LogValueType.APPLICATION,
            UUID.randomUUID(), 21)));
        response = replicationProtocol.handlePublishRequest(request.toMessage());
        assertThat(response.isAccepted(), is(true));
        assertThat(response.getLastReceivedMessageId(), is(21L));
        assertThat(response.getFlags().isEmpty(), is(true));
        checkLogEntries(logStore.getEntries(), request.getSource(), 2, 1, 21, true);

        request.setLogEntries(null);
        request.setFlags(Enums.of(MessageFlags.RESTART_SESSION));
        request.setLogEntries(Collections.singletonList(new LogEntry(0, new ByteArray(new byte[0]), LogValueType.APPLICATION,
            UUID.randomUUID(), 21)));
        response = replicationProtocol.handlePublishRequest(request.toMessage());
        assertThat(response.isAccepted(), is(true));
        assertThat(response.getLastReceivedMessageId(), is(21L));
        assertThat(response.getFlags().isEmpty(), is(true));

        request.setFlags(Enums.noneOf(MessageFlags.class));
        request.setLogEntries(Collections.singletonList(new LogEntry(0, new ByteArray(new byte[0]), LogValueType.APPLICATION,
            UUID.randomUUID(), 22)));
        response = replicationProtocol.handlePublishRequest(request.toMessage());
        assertThat(response.isAccepted(), is(true));
        assertThat(response.getLastReceivedMessageId(), is(22L));
        assertThat(response.getFlags().isEmpty(), is(true));
        checkLogEntries(logStore.getEntries(), request.getSource(), 2, 1, 22, true);
    }

    @Test
    public void testHandleAppendEntriesRequestHighTerm() {
        replicationProtocol.start();

        state.getServerState().setTerm(2);

        state.setRole(ServerRole.LEADER);
        AppendEntriesRequestBuilder request = createAppendEntriesRequest();
        request.setTerm(3);
        electionProtocol.setState(state);
        AppendEntriesResponse response = replicationProtocol.handleAppendEntriesRequest(request.toMessage());
        assertThat(response.isAccepted(), is(true));
        assertThat(response.getGroupId(), is(context.getGroupId()));
        assertThat(response.getSource(), is(context.getServerId()));
        assertThat(electionProtocol.getServerRole(), is(ServerRole.FOLLOWER));
    }

    @Test
    public void testHandleAppendEntriesRequestCandidate() {
        replicationProtocol.start();

        state.setRole(ServerRole.CANDIDATE);
        state.getServerState().setTerm(2);
        AppendEntriesRequestBuilder request = createAppendEntriesRequest();
        request.setTerm(2);
        replicationProtocol.handleAppendEntriesRequest(request.toMessage());
        assertThat(electionProtocol.getServerRole(), is(ServerRole.FOLLOWER));
    }

    @Test
    public void testHandleAppendEntriesRequestLeader() throws Throwable {
        replicationProtocol.start();

        state.setRole(ServerRole.LEADER);
        state.getServerState().setTerm(2);
        AppendEntriesRequestBuilder request = createAppendEntriesRequest();
        request.setTerm(2);
        new Expected(IllegalStateException.class, (Runnable) () -> replicationProtocol.handleAppendEntriesRequest(request.toMessage()));
    }

    @Test
    public void testHandleAppendEntriesRequestFollower() {
        replicationProtocol.start();

        state.setRole(ServerRole.FOLLOWER);
        state.getServerState().setTerm(2);
        AppendEntriesRequestBuilder request = createAppendEntriesRequest();
        request.setTerm(2);
        AppendEntriesResponse response = replicationProtocol.handleAppendEntriesRequest(request.toMessage());
        assertThat(electionProtocol.isElectionTimerRestarted(), is(true));
        assertThat(response.isAccepted(), is(true));
        assertThat(response.getGroupId(), is(context.getGroupId()));
        assertThat(response.getSource(), is(context.getServerId()));
        assertThat(response.getTerm(), is(2L));
    }

    @Test
    public void testHandleAppendEntriesRequestStateTransferSnapshotRequested() {
        replicationProtocol.start();

        state.setRole(ServerRole.FOLLOWER);
        state.getServerState().setTerm(2);
        AppendEntriesRequestBuilder request = createAppendEntriesRequest();
        request.setFlags(Enums.of(MessageFlags.STATE_TRANSFER_REQUIRED, MessageFlags.TRANSFER_SNAPSHOT));
        request.setLogEntries(Collections.singletonList(new LogEntry(0, Utils.writeStateTransferConfiguration(
            new StateTransferConfiguration("testHost", 1234, 1)), LogValueType.APPLICATION, null, 0)));
        AppendEntriesResponse response = replicationProtocol.handleAppendEntriesRequest(request.toMessage());
        assertThat(response.isAccepted(), is(false));
        assertThat(response.getFlags(), is(Enums.of(MessageFlags.STATE_TRANSFER_REQUIRED)));
        assertThat(stateTransferProtocol.isStateTransferRequested(), is(true));
        assertThat(stateTransferProtocol.getHost(), is("testHost"));
        assertThat(stateTransferProtocol.getPort(), is(1234));
        assertThat(stateTransferProtocol.getStartLogIndex(), is(1L));
    }

    @Test
    public void testHandleAppendEntriesRequestStateTransferLogRequested() {
        replicationProtocol.start();

        addLogEntries(UUID.randomUUID(), 2, 10);
        state.setRole(ServerRole.FOLLOWER);
        state.getServerState().setTerm(2);
        AppendEntriesRequestBuilder request = createAppendEntriesRequest();
        request.setLastLogIndex(5);
        request.setLastLogTerm(1);
        request.setFlags(Enums.of(MessageFlags.STATE_TRANSFER_REQUIRED, MessageFlags.TRANSFER_LOG));
        request.setLogEntries(Collections.singletonList(new LogEntry(0, Utils.writeStateTransferConfiguration(
                new StateTransferConfiguration("testHost", 1234, 0)), LogValueType.APPLICATION, null, 0)));
        AppendEntriesResponse response = replicationProtocol.handleAppendEntriesRequest(request.toMessage());
        assertThat(response.isAccepted(), is(false));
        assertThat(response.getFlags().contains(MessageFlags.STATE_TRANSFER_REQUIRED), is(false));
        assertThat(stateTransferProtocol.isStateTransferRequested(), is(false));

        request.setLastLogTerm(2);
        request.setLastLogIndex(5);
        response = replicationProtocol.handleAppendEntriesRequest(request.toMessage());
        assertThat(response.isAccepted(), is(false));
        assertThat(response.getFlags(), is(Enums.of(MessageFlags.STATE_TRANSFER_REQUIRED)));
        assertThat(stateTransferProtocol.isStateTransferRequested(), is(true));
        assertThat(stateTransferProtocol.getHost(), is("testHost"));
        assertThat(stateTransferProtocol.getPort(), is(1234));
        assertThat(stateTransferProtocol.getStartLogIndex(), is(6L));
    }

    @Test
    public void testHandleAppendEntriesRequestLogNotOk() {
        replicationProtocol.start();

        state.setRole(ServerRole.FOLLOWER);
        state.getServerState().setTerm(2);
        AppendEntriesRequestBuilder request = createAppendEntriesRequest();
        request.setTerm(1);
        AppendEntriesResponse response = replicationProtocol.handleAppendEntriesRequest(request.toMessage());
        assertThat(response.isAccepted(), is(false));
        assertThat(response.getNextIndex(), is(1L));
        assertThat(response.getTerm(), is(2L));

        UUID clientId = UUID.randomUUID();
        addLogEntries(clientId, 2, 10);

        request.setTerm(2);
        request.setLastLogTerm(1);
        request.setLastLogIndex(10);
        response = replicationProtocol.handleAppendEntriesRequest(request.toMessage());
        assertThat(response.isAccepted(), is(false));
        assertThat(response.getNextIndex(), is(11L));
        assertThat(response.getTerm(), is(2L));

        request.setTerm(2);
        request.setLastLogTerm(2);
        request.setLastLogIndex(11);
        response = replicationProtocol.handleAppendEntriesRequest(request.toMessage());
        assertThat(response.isAccepted(), is(false));

        request.setTerm(2);
        request.setLastLogIndex(0);
        response = replicationProtocol.handleAppendEntriesRequest(request.toMessage());
        assertThat(response.isAccepted(), is(true));

        request.setTerm(2);
        request.setLastLogTerm(2);
        request.setLastLogIndex(10);
        response = replicationProtocol.handleAppendEntriesRequest(request.toMessage());
        assertThat(response.isAccepted(), is(true));
    }

    @Test
    public void testHandleAppendEntriesRequestNormal() throws Exception {
        replicationProtocol.start();

        state.setRole(ServerRole.FOLLOWER);
        state.getServerState().setTerm(2);
        UUID clientId = UUID.randomUUID();
        addLogEntries(clientId, 2, 7);
        addLogEntries(clientId, 3, 3);

        Times.setTest(1000);
        AppendEntriesRequestBuilder request = createAppendEntriesRequest();
        request.setLogEntries(createLogEntries(clientId, 2, 6, 25));
        request.setTerm(2);
        request.setLastLogIndex(5);
        request.setLastLogTerm(2);
        request.setCommitIndex(10);
        AppendEntriesResponse response = replicationProtocol.handleAppendEntriesRequest(request.toMessage());
        assertThat(response.isAccepted(), is(true));
        assertThat(response.getNextIndex(), is(31L));
        assertThat(state.getLeader(), is(request.getSource()));

        LogStoreMock logStore = (LogStoreMock) context.getLogStore();
        checkLogEntries(logStore.getEntries(), clientId, 2, 10, 21, true);

        ClientSessionManager clientSessionManager = Tests.get(replicationProtocol, "clientSessionManager");
        ClientSession session = clientSessionManager.ensureSession(clientId);
        assertThat(session.getClientId(), is(clientId));
        assertThat(session.getLastReceivedMessageId(), is(10L));
        assertThat(session.getLastCommittedMessageId(), is(10L));
        assertThat(state.getServerState().getCommitIndex(), is(10L));
    }

    @Test
    public void testHandleAppendEntriesResponseAccepted() {
        replicationProtocol.start();
        startPeers();

        state.setRole(ServerRole.FOLLOWER);
        state.getServerState().setTerm(2);

        addLogEntries(UUID.randomUUID(), 2, 20);
        AppendEntriesResponseBuilder response = new AppendEntriesResponseBuilder();
        response.setGroupId(context.getGroupId());
        response.setAccepted(true);
        UUID peerId = state.getConfiguration().getServers().get(1).getId();
        IServerPeer peer = state.getPeers().get(peerId);
        response.setSource(UUID.randomUUID());
        response.setNextIndex(11);

        assertThat(state.getConfiguration().getServers().size(), is(10));

        for (int i = 2; i < state.getConfiguration().getServers().size(); i++) {
            IServerPeer serverPeer = state.getPeers().get(state.getConfiguration().getServers().get(i).getId());
            serverPeer.setMatchedIndex(i % 2 == 0 ? 10 : 9);
        }

        replicationProtocol.handleAppendEntriesResponse(response.toMessage());

        assertThat(peer.getNextLogIndex(), is(1L));

        state.setRole(ServerRole.LEADER);
        replicationProtocol.handleAppendEntriesResponse(response.toMessage());

        assertThat(peer.getNextLogIndex(), is(1L));

        response.setSource(peerId);
        replicationProtocol.handleAppendEntriesResponse(response.toMessage());
        assertThat(peer.getNextLogIndex(), is(11L));
        assertThat(peer.getMatchedIndex(), is(10L));
        assertThat(state.getServerState().getCommitIndex(), is(10L));
        assertThat(server.getRequests().size(), is(10));
        assertThat(server.getRequests().get(0).getValue() instanceof AppendEntriesRequest, is(true));
        server.getRequests().clear();

        for (int i = 2; i < state.getConfiguration().getServers().size(); i++) {
            IServerPeer serverPeer = state.getPeers().get(state.getConfiguration().getServers().get(i).getId());
            serverPeer.setMatchedIndex(20);
        }

        response.setNextIndex(21);
        replicationProtocol.handleAppendEntriesResponse(response.toMessage());
        assertThat(state.getServerState().getCommitIndex(), is(20L));
        assertThat(server.getRequests().size(), is(9));
    }

    @Test
    public void testHandleAppendEntriesResponseRejected() {
        replicationProtocol.start();
        startPeers();

        state.setRole(ServerRole.LEADER);
        state.getServerState().setTerm(2);

        addLogEntries(UUID.randomUUID(), 2, 20);
        AppendEntriesResponseBuilder response = new AppendEntriesResponseBuilder();
        response.setAccepted(false);
        response.setGroupId(context.getGroupId());
        UUID peerId = state.getConfiguration().getServers().get(1).getId();
        IServerPeer peer = state.getPeers().get(peerId);
        peer.setNextLogIndex(20);

        response.setSource(peerId);
        response.setNextIndex(11);

        replicationProtocol.handleAppendEntriesResponse(response.toMessage());
        assertThat(peer.getNextLogIndex(), is(11L));
        assertThat(server.getRequests().isEmpty(), is(false));
        server.getRequests().clear();

        response.setNextIndex(12);
        replicationProtocol.handleAppendEntriesResponse(response.toMessage());
        assertThat(peer.getNextLogIndex(), is(10L));
        assertThat(server.getRequests().isEmpty(), is(false));
        server.getRequests().clear();

        replicationProtocol.handleAppendEntriesResponse(response.toMessage());
        assertThat(peer.getNextLogIndex(), is(8L));
        assertThat(server.getRequests().isEmpty(), is(false));
        server.getRequests().clear();

        replicationProtocol.handleAppendEntriesResponse(response.toMessage());
        assertThat(peer.getNextLogIndex(), is(4L));
        assertThat(server.getRequests().isEmpty(), is(false));
        server.getRequests().clear();

        response.setFlags(Enums.of(MessageFlags.STATE_TRANSFER_REQUIRED));
        replicationProtocol.handleAppendEntriesResponse(response.toMessage());
        assertThat(peer.getNextLogIndex(), is(4L));
        assertThat(server.getRequests().isEmpty(), is(true));
    }

    @Test
    public void testRemoteCommit() throws Exception {
        replicationProtocol.start();
        startPeers();

        state.setRole(ServerRole.FOLLOWER);
        state.getServerState().setTerm(2);
        addLogEntries(UUID.randomUUID(), 2, 10);

        Tests.invoke(replicationProtocol, "commit", 5L);
        assertThat(state.getQuickCommitIndex(), is(5L));
        assertThat(server.getRequests().isEmpty(), is(true));

        state.setRole(ServerRole.LEADER);
        Tests.invoke(replicationProtocol, "commit", 5L);
        assertThat(state.getQuickCommitIndex(), is(5L));
        assertThat(server.getRequests().isEmpty(), is(true));

        Tests.invoke(replicationProtocol, "commit", 6L);
        assertThat(state.getQuickCommitIndex(), is(6L));
        assertThat(server.getRequests().size(), is(state.getConfiguration().getServers().size() - 1));
        for (Pair<IServerPeer, ServerRequest> request : server.getRequests())
            assertThat(((AppendEntriesRequest)request.getValue()).getCommitIndex(), is(6L));

        assertThat(state.getServerState().getCommitIndex(), is(6L));
    }

    @Test
    public void testLocalCommitData() throws Exception {
        replicationProtocol.start();

        StateMachineTransactionMock transaction = (StateMachineTransactionMock) context.getStateMachine().beginTransaction(true);
        state.setRole(ServerRole.FOLLOWER);
        state.getServerState().setTerm(2);
        UUID clientId = UUID.randomUUID();
        addLogEntries(clientId, 2, 10);
        state.getServerState().setCommitIndex(6);
        state.setQuickCommitIndex(5);

        Tests.invoke(replicationProtocol, "localCommit");
        assertThat(state.getServerState().getCommitIndex(), is(6L));

        state.getServerState().setCommitIndex(11);
        Tests.invoke(replicationProtocol, "localCommit");
        assertThat(state.getServerState().getCommitIndex(), is(11L));

        Tests.set(state.getServerState(), "commitIndex", 0);
        state.setQuickCommitIndex(5);
        Tests.invoke(replicationProtocol, "localCommit");
        assertThat(state.getServerState().getCommitIndex(), is(5L));
        assertThat(transaction.getCommits().size(), is(5));

        state.setQuickCommitIndex(11);
        Tests.invoke(replicationProtocol, "localCommit");
        assertThat(state.getServerState().getCommitIndex(), is(10L));
        assertThat(transaction.getCommits().size(), is(10));

        ClientSessionManager clientSessionManager = Tests.get(replicationProtocol, "clientSessionManager");
        ClientSession session = clientSessionManager.ensureSession(clientId);
        assertThat(session.getClientId(), is(clientId));
        assertThat(session.getLastReceivedMessageId(), is(10L));
        assertThat(session.getLastCommittedMessageId(), is(10L));

        assertThat(transaction.isCommitted(), is(true));
        assertThat(context.getLogStore().getCommitIndex(), is(10L));
    }

    @Test
    public void testLocalCommitConfiguration() throws Exception {
        replicationProtocol.start();

        StateMachineTransactionMock transaction = (StateMachineTransactionMock) context.getStateMachine().beginTransaction(true);
        state.setRole(ServerRole.FOLLOWER);
        state.getServerState().setTerm(2);
        GroupConfiguration oldConfiguration = state.getConfiguration();
        List<ServerConfiguration> servers = new ArrayList<>(oldConfiguration.getServers());
        ServerConfiguration newServer = new ServerConfiguration(UUID.randomUUID(), "testEndpoint");
        servers.add(newServer);
        GroupConfiguration newConfiguration = new GroupConfiguration(oldConfiguration.getName(), oldConfiguration.getGroupId(), servers, false);

        context.getLogStore().append(new LogEntry(2, Utils.writeGroupConfiguration(newConfiguration), LogValueType.CONFIGURATION, null, 0));
        state.setQuickCommitIndex(1);
        state.setConfigurationChanging(true);
        state.setCatchingUp(true);

        Tests.invoke(replicationProtocol, "localCommit");
        assertThat(state.getServerState().getCommitIndex(), is(1L));
        assertThat(membershipProtocol.getNewConfiguration(), is(newConfiguration));
        assertThat(transaction.isCommitted(), is(true));
        assertThat(context.getLogStore().getCommitIndex(), is(1L));
    }

    @Test
    public void testCommitLock() throws Exception {
        replicationProtocol.start();

        replicationProtocol.lockCommits();

        StateMachineTransactionMock transaction = (StateMachineTransactionMock) context.getStateMachine().beginTransaction(true);
        transaction.reset();
        state.setRole(ServerRole.FOLLOWER);
        state.getServerState().setTerm(2);
        UUID clientId = UUID.randomUUID();
        addLogEntries(clientId, 2, 10);
        state.setQuickCommitIndex(5);

        Tests.invoke(replicationProtocol, "localCommit");
        assertThat(transaction.isCommitted(), is(false));

        replicationProtocol.unlockCommits();

        Tests.invoke(replicationProtocol, "localCommit");
        assertThat(state.getServerState().getCommitIndex(), is(5L));
        assertThat(transaction.isCommitted(), is(true));
        assertThat(transaction.getCommitCount(), is(1));
        assertThat(context.getLogStore().getCommitIndex(), is(5L));
    }

    @Test
    public void testUpdateClientSessions() throws Exception {
        replicationProtocol.start();

        state.setRole(ServerRole.LEADER);
        state.getServerState().setTerm(2);
        UUID clientId = UUID.randomUUID();
        addLogEntries(clientId, 2, 10);
        state.setQuickCommitIndex(5);

        Tests.invoke(replicationProtocol, "localCommit");
        replicationProtocol.onLeader();

        ClientSessionManager clientSessionManager = Tests.get(replicationProtocol, "clientSessionManager");
        ClientSession session = clientSessionManager.ensureSession(clientId);
        assertThat(session.getClientId(), is(clientId));
        assertThat(session.getLastReceivedMessageId(), is(10L));
        assertThat(session.getLastCommittedMessageId(), is(5L));
    }

    @Test
    public void testSessionManager() throws Throwable {
        Times.setTest(1000);

        ClientSessionManager sessionManager = new ClientSessionManager(null, 10000);
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        UUID id3 = UUID.randomUUID();
        ClientSession session1 = sessionManager.ensureSession(id1);
        ClientSession session2 = sessionManager.ensureSession(id2);
        ClientSession session3 = sessionManager.ensureSession(id3);
        assertThat(session1.getClientId(), is(id1));
        assertThat(session1.getLastAccessTime(), is(1000L));
        assertThat(session2.getClientId(), is(id2));
        assertThat(session3.getClientId(), is(id3));

        Times.setTest(5000);
        assertThat(sessionManager.ensureSession(id2) == session2, is(true));
        assertThat(session1.getLastAccessTime(), is(1000L));
        assertThat(session2.getLastAccessTime(), is(5000L));

        sessionManager.onTimer(11000);
        SimpleList<ClientSession> sessions = Tests.get(sessionManager, "sessions");
        Map<UUID, ClientSession> sessionsMap = Tests.get(sessionManager, "sessionsMap");
        assertThat(sessions.toList(), is(Collections.singletonList(session2)));
        assertThat(sessionsMap, is(Collections.singletonMap(session2.getClientId(), session2)));

        assertThat(session2.receive(1), is(true));
        assertThat(session2.getLastReceivedMessageId(), is(1L));
        assertThat(session2.getLastCommittedMessageId(), is(0L));
        assertThat(session2.receive(1), is(false));
        assertThat(session2.isOutOfOrderReceived(), is(false));
        assertThat(session2.receive(2), is(true));
        assertThat(session2.getLastReceivedMessageId(), is(2L));
        assertThat(session2.getLastCommittedMessageId(), is(0L));
        assertThat(session2.receive(4), is(false));
        assertThat(session2.isOutOfOrderReceived(), is(true));

        assertThat(session2.receive(3), is(true));
        assertThat(session2.getLastReceivedMessageId(), is(3L));
        assertThat(session2.getLastCommittedMessageId(), is(0L));
        assertThat(session2.isOutOfOrderReceived(), is(false));

        session2.commit(1);
        assertThat(session2.getLastCommittedMessageId(), is(1L));
        session2.commit(2);
        assertThat(session2.getLastCommittedMessageId(), is(2L));
        new Expected(IllegalStateException.class, (Runnable) () -> session2.commit(1));
        new Expected(IllegalStateException.class, (Runnable) () -> session2.commit(4));
        session2.commit(3);
        assertThat(session2.getLastCommittedMessageId(), is(3L));

        session2.setOutOfOrderReceived(true);

        session2.commit(4);
        assertThat(session2.getLastCommittedMessageId(), is(4L));
        assertThat(session2.getLastReceivedMessageId(), is(4L));
        assertThat(session2.isOutOfOrderReceived(), is(false));
    }

    @Test
    public void testOnFollower() {
        for (IServerPeer peer : state.getPeers().values())
            peer.start();

        replicationProtocol.onFollower();

        for (IServerPeer peer : state.getPeers().values())
            assertThat(((ServerPeerMock) peer).isHeartbeatEnabled(), is(false));
    }

    @Test
    public void testAddInitialConfiguration() {
        state.setRole(ServerRole.LEADER);

        replicationProtocol.onLeader();

        assertThat(context.getLogStore().getEndIndex(), is(2L));
        assertThat(context.getLogStore().getAt(1).getValueType(), is(LogValueType.CONFIGURATION));
        assertThat(Utils.readGroupConfiguration(context.getLogStore().getAt(1).getValue()), is(state.getConfiguration()));
        assertThat(state.isConfigurationChanging(), is(true));
    }

    @Test
    public void testOnLeader() {
        state.setRole(ServerRole.LEADER);
        state.setQuickCommitIndex(1);

        replicationProtocol.onLeader();

        for (IServerPeer peer : state.getPeers().values()) {
            ServerPeerMock serverPeer = (ServerPeerMock) peer;
            assertThat(serverPeer.getNextLogIndex(), is(1));
            assertThat(serverPeer.isHeartbeatEnabled(), is(true));
        }
    }

    @Test
    public void testInitClientSessionsOnLeader() throws Throwable {
        UUID clientId = UUID.randomUUID();

        state.setRole(ServerRole.LEADER);
        addLogEntries(clientId, 2, 10);
        state.setQuickCommitIndex(5);

        replicationProtocol.onLeader();
        ClientSessionManager clientSessionManager = Tests.get(replicationProtocol, "clientSessionManager");
        ClientSession session = clientSessionManager.ensureSession(clientId);
        assertThat(session.getLastReceivedMessageId(), is(10L));
    }

    @Test
    public void testInitUncommittedConfigurationOnLeader() throws Throwable {
        state.setRole(ServerRole.LEADER);
        addLogEntries(UUID.randomUUID(), 2, 10);
        state.setQuickCommitIndex(5);
        context.getLogStore().append(new LogEntry(2, Utils.writeGroupConfiguration(state.getConfiguration()),
            LogValueType.CONFIGURATION, null, 0));
        replicationProtocol.onLeader();

        assertThat(membershipProtocol.getNewConfiguration(), is(state.getConfiguration()));
    }

    private void startPeers() {
        Times.setTest(1000);
        for (int i = 1; i < state.getConfiguration().getServers().size(); i++) {
            IServerPeer serverPeer = state.getPeers().get(state.getConfiguration().getServers().get(i).getId());
            serverPeer.start();
            serverPeer.canHeartbeat(Times.getCurrentTime());
        }

        Times.setTest(2000);
        for (int i = 1; i < state.getConfiguration().getServers().size(); i++) {
            IServerPeer serverPeer = state.getPeers().get(state.getConfiguration().getServers().get(i).getId());
            serverPeer.canHeartbeat(Times.getCurrentTime());
        }
    }
}