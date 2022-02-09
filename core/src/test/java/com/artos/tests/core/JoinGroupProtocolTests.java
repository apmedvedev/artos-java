package com.artos.tests.core;

import com.artos.api.core.server.conf.ServerChannelFactoryConfigurationBuilder;
import com.artos.api.core.server.conf.ServerConfiguration;
import com.artos.impl.core.message.JoinGroupRequest;
import com.artos.impl.core.message.JoinGroupRequestBuilder;
import com.artos.impl.core.message.JoinGroupResponse;
import com.artos.impl.core.message.JoinGroupResponseBuilder;
import com.artos.impl.core.message.MessageFlags;
import com.artos.impl.core.server.impl.ServerRole;
import com.artos.impl.core.server.membership.JoinGroupProtocol;
import com.artos.impl.core.server.state.StateTransferConfiguration;
import com.artos.impl.core.server.utils.Utils;
import com.artos.spi.core.LogEntry;
import com.artos.spi.core.LogValueType;
import com.artos.tests.core.mocks.FollowerMessageSenderMock;
import com.artos.tests.core.mocks.LogStoreMock;
import com.artos.tests.core.mocks.MembershipProtocolMock;
import com.artos.tests.core.mocks.ReplicationProtocolMock;
import com.artos.tests.core.mocks.StateTransferProtocolMock;
import com.exametrika.common.compartment.ICompartmentTimer;
import com.exametrika.common.tests.Tests;
import com.exametrika.common.utils.Enums;
import com.exametrika.common.utils.Times;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class JoinGroupProtocolTests extends AbstractRaftTest {
    private JoinGroupProtocol joinGroupProtocol;
    private MembershipProtocolMock membershipProtocol;
    private ReplicationProtocolMock replicationProtocol;
    private StateTransferProtocolMock stateTransferProtocol;
    private FollowerMessageSenderMock messageSender;

    @Before
    public void setUp() throws Throwable {
        Times.setTest(0);

        createState(10);
        context = createContext(state.getConfiguration());

        ServerChannelFactoryConfigurationBuilder builder = new ServerChannelFactoryConfigurationBuilder();
        builder.setJoinGroupPeriod(1000).setMinStateTransferGapLogEntryCount(5);
        Tests.set(context, "serverChannelFactoryConfiguration", builder.toConfiguration());

        membershipProtocol = new MembershipProtocolMock();
        replicationProtocol = new ReplicationProtocolMock();
        stateTransferProtocol = new StateTransferProtocolMock();
        messageSender = new FollowerMessageSenderMock();

        joinGroupProtocol = new JoinGroupProtocol(context, state);
        joinGroupProtocol.setMembershipProtocol(membershipProtocol);
        joinGroupProtocol.setReplicationProtocol(replicationProtocol);
        joinGroupProtocol.setStateTransferProtocol(stateTransferProtocol);
        state.setCatchingUp(true);

        joinGroupProtocol.start();
    }

    @Test
    public void testRequestJoinGroupAlreadyJoined() throws Throwable {
        state.getServerState().setTerm(2);

        addLogEntries(UUID.randomUUID(), 2, 10);
        ICompartmentTimer timer = Tests.get(joinGroupProtocol, "joinGroupTimer");

        JoinGroupResponseBuilder response = new JoinGroupResponseBuilder();
        response.setGroupId(context.getGroupId());
        response.setSource(UUID.randomUUID());
        response.setAccepted(true);
        response.setFlags(Enums.of(MessageFlags.ALREADY_JOINED));

        messageSender.setResponse(response.toMessage());
        Times.setTest(1000);
        joinGroupProtocol.onTimer(1000);

        assertThat(timer.isStarted(), is(false));
        assertThat(state.isCatchingUp(), is(false));

        JoinGroupRequest request = (JoinGroupRequest) messageSender.getRequest();
        assertThat(request.getSource(), is(context.getServerId()));
        assertThat(request.getGroupId(), is(context.getGroupId()));
        assertThat(request.getLastLogTerm(), is(2L));
        assertThat(request.getLastLogIndex(), is(10L));
        assertThat(request.getConfiguration() != null, is(true));
        assertThat(request.getConfiguration(), is(state.getConfiguration().findServer(context.getServerId())));
    }

    @Test
    public void testRequestJoinGroupStateTransfer() throws Throwable {
        state.setCatchingUp(false);
        state.getServerState().setTerm(2);

        addLogEntries(UUID.randomUUID(), 2, 10);
        ICompartmentTimer timer = Tests.get(joinGroupProtocol, "joinGroupTimer");

        JoinGroupResponseBuilder response = new JoinGroupResponseBuilder();
        response.setGroupId(context.getGroupId());
        response.setSource(UUID.randomUUID());
        response.setAccepted(true);
        response.setFlags(Enums.of(MessageFlags.CATCHING_UP, MessageFlags.STATE_TRANSFER_REQUIRED));
        response.setStateTransferConfiguration(new StateTransferConfiguration("testHost", 1234, 5678));

        messageSender.setResponse(response.toMessage());
        stateTransferProtocol.setBlockResponse(true);
        Times.setTest(1000);
        joinGroupProtocol.onTimer(1000);

        assertThat(timer.isStarted(), is(false));
        assertThat(state.isCatchingUp(), is(true));
        assertThat(stateTransferProtocol.isStateTransferRequested(), is(true));
        assertThat(stateTransferProtocol.getHost(), is("testHost"));
        assertThat(stateTransferProtocol.getPort(), is(1234));
        assertThat(stateTransferProtocol.getStartLogIndex(), is(5678L));
        stateTransferProtocol.reset();
        stateTransferProtocol.getCompletionHandler().onSucceeded(response.toMessage());
        assertThat(timer.isStarted(), is(true));

        Times.setTest(2000);
        joinGroupProtocol.onTimer(2000);

        assertThat(timer.isStarted(), is(false));
        assertThat(state.isCatchingUp(), is(true));
        assertThat(stateTransferProtocol.isStateTransferRequested(), is(true));
        stateTransferProtocol.getCompletionHandler().onFailed(new RuntimeException("testError"));
        assertThat(timer.isStarted(), is(true));
    }

    @Test
    public void testRequestJoinGroupRewindRepeat() throws Throwable {
        state.setCatchingUp(false);
        state.getServerState().setTerm(2);

        addLogEntries(UUID.randomUUID(), 2, 10);
        ICompartmentTimer timer = Tests.get(joinGroupProtocol, "joinGroupTimer");

        JoinGroupResponseBuilder response = new JoinGroupResponseBuilder();
        response.setGroupId(context.getGroupId());
        response.setSource(UUID.randomUUID());
        response.setAccepted(true);
        response.setFlags(Enums.of(MessageFlags.CATCHING_UP, MessageFlags.REWIND_REPEAT_JOIN));

        messageSender.setResponse(response.toMessage());
        stateTransferProtocol.setBlockResponse(true);
        Times.setTest(1000);
        joinGroupProtocol.onTimer(1000);

        assertThat(timer.isStarted(), is(true));
        assertThat(state.isCatchingUp(), is(true));

        Times.setTest(2000);
        joinGroupProtocol.onTimer(2000);

        JoinGroupRequest request = (JoinGroupRequest) messageSender.getRequest();
        assertThat(request.getSource(), is(context.getServerId()));
        assertThat(request.getGroupId(), is(context.getGroupId()));
        assertThat(request.getLastLogTerm(), is(2L));
        assertThat(request.getLastLogIndex(), is(9L));

        assertThat(Tests.get(joinGroupProtocol, "rewindStep"), is(4L));
    }

    @Test
    public void testHandleJoinGroupRequest() {
        addLogEntries(UUID.randomUUID(), 2, 10);
        state.getServerState().setTerm(2);

        JoinGroupRequestBuilder request = new JoinGroupRequestBuilder();
        request.setGroupId(context.getGroupId());
        request.setSource(context.getServerId());
        request.setConfiguration(new ServerConfiguration(UUID.randomUUID(), "testEnpoint"));

        JoinGroupResponse response = joinGroupProtocol.handleJoinGroupRequest(request.toMessage());
        assertThat(response.getSource(), is(context.getServerId()));
        assertThat(response.getGroupId(), is(context.getGroupId()));
        assertThat(response.isAccepted(), is(false));

        state.setRole(ServerRole.LEADER);
        state.setConfigurationChanging(true);
        response = joinGroupProtocol.handleJoinGroupRequest(request.toMessage());
        assertThat(response.isAccepted(), is(false));

        state.setConfigurationChanging(false);
        response = joinGroupProtocol.handleJoinGroupRequest(request.toMessage());
        assertThat(response.isAccepted(), is(true));
        assertThat(response.getFlags().contains(MessageFlags.ALREADY_JOINED), is(true));

        ((LogStoreMock)context.getLogStore()).setStartIndex(5);
        request.setSource(UUID.randomUUID());
        request.setLastLogIndex(3);
        stateTransferProtocol.setHost("testHost");
        stateTransferProtocol.setPort(1234);
        stateTransferProtocol.setStartLogIndex(5678);
        response = joinGroupProtocol.handleJoinGroupRequest(request.toMessage());
        assertThat(response.isAccepted(), is(true));
        assertThat(response.getFlags().contains(MessageFlags.CATCHING_UP), is(true));
        assertThat(response.getFlags().contains(MessageFlags.STATE_TRANSFER_REQUIRED), is(true));
        StateTransferConfiguration configuration = response.getStateTransferConfiguration();
        assertThat(configuration.getHost(), is("testHost"));
        assertThat(configuration.getPort(), is(1234));
        assertThat(configuration.getStartLogIndex(), is(1L));

        request.setLastLogIndex(9);
        request.setLastLogTerm(2);
        response = joinGroupProtocol.handleJoinGroupRequest(request.toMessage());
        assertThat(response.isAccepted(), is(true));
        assertThat(response.getFlags().contains(MessageFlags.CATCHING_UP), is(true));
        assertThat(response.getFlags().contains(MessageFlags.STATE_TRANSFER_REQUIRED), is(true));
        configuration = response.getStateTransferConfiguration();
        assertThat(configuration.getHost(), is("testHost"));
        assertThat(configuration.getPort(), is(1234));
        assertThat(configuration.getStartLogIndex(), is(10L));

        request.setLastLogIndex(15);
        request.setLastLogTerm(1);
        response = joinGroupProtocol.handleJoinGroupRequest(request.toMessage());
        assertThat(response.isAccepted(), is(true));
        assertThat(response.getFlags().contains(MessageFlags.CATCHING_UP), is(true));
        assertThat(response.getFlags().contains(MessageFlags.REWIND_REPEAT_JOIN), is(true));

        ServerConfiguration newServer = new ServerConfiguration(UUID.randomUUID(), "newServer");
        request.setLastLogIndex(14);
        request.setLastLogTerm(2);
        request.setConfiguration(newServer);
        response = joinGroupProtocol.handleJoinGroupRequest(request.toMessage());
        assertThat(response.isAccepted(), is(true));
        assertThat(membershipProtocol.getNewConfiguration().findServer(newServer.getId()), is(newServer));
        assertThat(replicationProtocol.isAppendEntriesRequested(), is(true));
        assertThat(response.getFlags().contains(MessageFlags.CATCHING_UP), is(true));

        LogEntry logEntry = context.getLogStore().getAt(context.getLogStore().getEndIndex() - 1);
        assertThat(logEntry.getValueType(), is(LogValueType.CONFIGURATION));
        assertThat(logEntry.getTerm(), is(2L));
        assertThat(Utils.readGroupConfiguration(logEntry.getValue()), is(membershipProtocol.getNewConfiguration()));
    }
}
