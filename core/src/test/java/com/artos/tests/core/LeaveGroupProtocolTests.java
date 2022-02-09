package com.artos.tests.core;

import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.api.core.server.conf.ServerChannelFactoryConfigurationBuilder;
import com.artos.api.core.server.conf.ServerConfiguration;
import com.artos.impl.core.message.LeaveGroupRequest;
import com.artos.impl.core.message.LeaveGroupRequestBuilder;
import com.artos.impl.core.message.ServerResponse;
import com.artos.impl.core.server.impl.ServerRole;
import com.artos.impl.core.server.membership.LeaveGroupProtocol;
import com.artos.impl.core.server.utils.Utils;
import com.artos.spi.core.LogEntry;
import com.artos.spi.core.LogValueType;
import com.artos.tests.core.mocks.CompletionHandlerMock;
import com.artos.tests.core.mocks.ElectionProtocolMock;
import com.artos.tests.core.mocks.FollowerMessageSenderMock;
import com.artos.tests.core.mocks.MembershipProtocolMock;
import com.artos.tests.core.mocks.ReplicationProtocolMock;
import com.artos.tests.core.mocks.ServerPeerMock;
import com.exametrika.common.compartment.ICompartmentTimer;
import com.exametrika.common.tests.Tests;
import com.exametrika.common.utils.Times;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

public class LeaveGroupProtocolTests extends AbstractRaftTest {
    private LeaveGroupProtocol leaveGroupProtocol;
    private MembershipProtocolMock membershipProtocol;
    private ReplicationProtocolMock replicationProtocol;
    private ElectionProtocolMock electionProtocol;
    private FollowerMessageSenderMock messageSender;

    @Before
    public void setUp() throws Throwable {
        Times.setTest(0);

        createState(10);
        context = createContext(state.getConfiguration());

        ServerChannelFactoryConfigurationBuilder builder = new ServerChannelFactoryConfigurationBuilder();
        builder.setLeaveGroupPeriod(1000).setLeaveGroupAttemptCount(1).setAutoLeaveGroupTimeout(10000);
        Tests.set(context, "serverChannelFactoryConfiguration", builder.toConfiguration());

        membershipProtocol = new MembershipProtocolMock();
        replicationProtocol = new ReplicationProtocolMock();
        electionProtocol = new ElectionProtocolMock();
        messageSender = new FollowerMessageSenderMock();

        leaveGroupProtocol = new LeaveGroupProtocol(context, state);
        leaveGroupProtocol.setMembershipProtocol(membershipProtocol);
        leaveGroupProtocol.setReplicationProtocol(replicationProtocol);
        leaveGroupProtocol.setElectionProtocol(electionProtocol);

        leaveGroupProtocol.start();
    }

    @Test
    public void testHandleLeaveGroupRequest() {
        state.getServerState().setTerm(2);
        ServerConfiguration removedServer = state.getConfiguration().getServers().get(1);
        LeaveGroupRequestBuilder request = new LeaveGroupRequestBuilder();
        request.setGroupId(context.getGroupId());
        request.setSource(removedServer.getId());

        ServerResponse response = leaveGroupProtocol.handleLeaveGroupRequest(request.toMessage());
        assertThat(response.isAccepted(), is(false));

        state.setConfigurationChanging(true);
        state.setRole(ServerRole.LEADER);
        response = leaveGroupProtocol.handleLeaveGroupRequest(request.toMessage());
        assertThat(response.isAccepted(), is(false));

        state.setConfigurationChanging(false);
        response = leaveGroupProtocol.handleLeaveGroupRequest(request.toMessage());
        assertThat(response.isAccepted(), is(true));
        assertThat(state.isConfigurationChanging(), is(true));
        assertThat(replicationProtocol.isAppendEntriesRequested(), is(true));

        LogEntry logEntry = context.getLogStore().getAt(context.getLogStore().getEndIndex() - 1);
        assertThat(logEntry.getValueType(), is(LogValueType.CONFIGURATION));
        assertThat(logEntry.getTerm(), is(2L));
        GroupConfiguration configuration = Utils.readGroupConfiguration(logEntry.getValue());
        assertThat(configuration.findServer(removedServer.getId()), nullValue());
    }

    @Test
    public void testGracefulClose() {
        CompletionHandlerMock completionHandler = new CompletionHandlerMock();
        leaveGroupProtocol.gracefulClose(completionHandler);
        assertThat(completionHandler.getSucceeded() != null, is(true));
        completionHandler.reset();

        state.setRole(ServerRole.LEADER);
        for (int i = 0; i < state.getConfiguration().getServers().size(); i++) {
            ServerConfiguration configuration = state.getConfiguration().getServers().get(i);
            if (!configuration.getId().equals(context.getServerId())) {
                ServerPeerMock peer = new ServerPeerMock();
                peer.setConfiguration(new ServerConfiguration(configuration.getId(), configuration.getEndpoint()));
                peer.start();
                peer.setMatchedIndex(i);
                state.addPeer(peer);
            }
        }

        leaveGroupProtocol.gracefulClose(completionHandler);
        assertThat(completionHandler.getSucceeded() != null, is(true));
    }

    @Test
    public void testLeaveGroup() throws Throwable {
        CompletionHandlerMock completionHandler = new CompletionHandlerMock();
        leaveGroupProtocol.leaveGroup(completionHandler);
        assertThat(completionHandler.getFailed() != null, is(true));
        completionHandler.reset();

        state.setRole(ServerRole.LEADER);
        for (int i = 0; i < state.getConfiguration().getServers().size(); i++) {
            ServerConfiguration configuration = state.getConfiguration().getServers().get(i);
            if (!configuration.getId().equals(context.getServerId())) {
                ServerPeerMock peer = new ServerPeerMock();
                peer.setConfiguration(new ServerConfiguration(configuration.getId(), configuration.getEndpoint()));
                peer.start();
                peer.setMatchedIndex(i);
                state.addPeer(peer);
            }
        }

        leaveGroupProtocol.leaveGroup(completionHandler);
        state.setRole(ServerRole.FOLLOWER);
        assertThat(completionHandler.getFailed() != null, is(true));
        completionHandler.reset();

        ICompartmentTimer timer = Tests.get(leaveGroupProtocol, "leaveGroupTimer");
        assertThat(timer.isStarted(), is(true));
        assertThat(timer.getPeriod(), is(1000L));
        assertThat(Tests.get(leaveGroupProtocol, "leaveGroupCompletionHandler") == completionHandler, is(true));

        Times.setTest(1000);
        leaveGroupProtocol.onTimer(1000);

        LeaveGroupRequest request = (LeaveGroupRequest) messageSender.getRequest();
        assertThat(request.getSource(), is(context.getServerId()));
        assertThat(request.getGroupId(), is(context.getGroupId()));

        Times.setTest(2000);
        leaveGroupProtocol.onTimer(2000);
        assertThat(completionHandler.getFailed() != null, is(true));
        assertThat(timer.isStarted(), is(false));
        assertThat(Tests.get(leaveGroupProtocol, "leaveGroupCompletionHandler"), nullValue());
        completionHandler.reset();

        leaveGroupProtocol.leaveGroup(completionHandler);
        timer = Tests.get(leaveGroupProtocol, "leaveGroupTimer");
        assertThat(timer.isStarted(), is(true));
        assertThat(timer.getPeriod(), is(1000L));
        assertThat(Tests.get(leaveGroupProtocol, "leaveGroupCompletionHandler") == completionHandler, is(true));
        leaveGroupProtocol.onLeft();

        assertThat(completionHandler.getSucceeded() != null, is(true));
        assertThat(timer.isStarted(), is(false));
        assertThat(Tests.get(leaveGroupProtocol, "leaveGroupCompletionHandler"), nullValue());
    }

    @Test
    public void testAutoLeaveGroupTimeout() throws Throwable {
        state.setRole(ServerRole.LEADER);
        state.getServerState().setTerm(2);
        leaveGroupProtocol.onLeader();

        ICompartmentTimer timer = Tests.get(leaveGroupProtocol, "autoLeaveGroupTimeout");
        assertThat(timer.isStarted(), is(true));
        assertThat(timer.getPeriod(), is(10000L));

        for (int i = 0; i < state.getConfiguration().getServers().size(); i++) {
            ServerConfiguration configuration = state.getConfiguration().getServers().get(i);
            if (!configuration.getId().equals(context.getServerId())) {
                ServerPeerMock peer = new ServerPeerMock();
                peer.setConfiguration(new ServerConfiguration(configuration.getId(), configuration.getEndpoint()));
                peer.setLastResponseTime(10000);
                state.addPeer(peer);
            }
        }

        state.setConfigurationChanging(true);
        Times.setTest(11000);
        leaveGroupProtocol.onTimer(11000);

        assertThat(context.getLogStore().getEndIndex(), is(1L));
        state.setConfigurationChanging(false);
        Times.setTest(21000);
        leaveGroupProtocol.onTimer(21000);

        assertThat(state.isConfigurationChanging(), is(true));
        assertThat(replicationProtocol.isAppendEntriesRequested(), is(true));

        LogEntry logEntry = context.getLogStore().getAt(context.getLogStore().getEndIndex() - 1);
        assertThat(logEntry.getValueType(), is(LogValueType.CONFIGURATION));
        assertThat(logEntry.getTerm(), is(2L));
        GroupConfiguration configuration = Utils.readGroupConfiguration(logEntry.getValue());
        assertThat(configuration.getServers().size(), is(1));
        assertThat(configuration.findServer(context.getServerId()) != null, is(true));
    }
}
