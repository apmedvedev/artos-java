package com.artos.tests.core;

import com.artos.api.core.server.conf.ServerChannelFactoryConfigurationBuilder;
import com.artos.api.core.server.conf.ServerConfiguration;
import com.artos.impl.core.message.ServerResponse;
import com.artos.impl.core.message.VoteRequestBuilder;
import com.artos.impl.core.message.VoteResponseBuilder;
import com.artos.impl.core.server.election.ElectionProtocol;
import com.artos.impl.core.server.impl.IServer;
import com.artos.impl.core.server.impl.ServerRole;
import com.artos.tests.core.mocks.MembershipProtocolMock;
import com.artos.tests.core.mocks.ServerMock;
import com.artos.tests.core.mocks.ServerPeerMock;
import com.artos.tests.core.mocks.StateMachineMock;
import com.artos.tests.core.mocks.StateMachineTransactionMock;
import com.exametrika.common.compartment.ICompartmentTimer;
import com.exametrika.common.tests.Tests;
import com.exametrika.common.utils.Times;
import org.hamcrest.core.Is;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ElectionProtocolTests extends AbstractRaftTest {
    private ElectionProtocol electionProtocol;
    private MembershipProtocolMock membershipProtocol;

    @Before
    public void setUp() throws Throwable {
        Times.setTest(0);

        createState(10);
        context = createContext(state.getConfiguration());
        IServer server = new ServerMock();

        membershipProtocol = new MembershipProtocolMock();

        ServerChannelFactoryConfigurationBuilder builder = new ServerChannelFactoryConfigurationBuilder();
        builder.setMinElectionTimeout(5000).setMaxElectionTimeout(10000);
        Tests.set(context, "serverChannelFactoryConfiguration", builder.toConfiguration());

        electionProtocol = new ElectionProtocol(server, context, state);
        electionProtocol.setMembershipProtocol(membershipProtocol);
    }

    @Test
    public void testSinglePeerLeaderElection() throws Exception {
        state.setCatchingUp(false);
        electionProtocol.start();

        Times.setTest(4000);
        electionProtocol.onTimer(4000);
        assertThat(state.getRole(), Is.is(ServerRole.FOLLOWER));

        Times.setTest(10000);
        electionProtocol.onTimer(10000);
        assertThat(state.getRole(), is(ServerRole.LEADER));
        assertThat(state.getLeader(), Is.is(context.getServerId()));
        assertThat(membershipProtocol.isOnLeader(), is(true));
        checkElectionTimer(false);

        Times.setTest(20000);
        electionProtocol.onTimer(20000);
        assertThat(state.getRole(), is(ServerRole.LEADER));

        assertThat(membershipProtocol.isOnLeader(), is(true));

        StateMachineTransactionMock transaction = ((StateMachineMock) context.getStateMachine()).getTransaction();
        assertThat(transaction.isCommitted(), is(true));
    }

    @Test
    public void testMultiPeerLeaderElection() throws Exception {
        state.setCatchingUp(false);
        List<ServerPeerMock> peers = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            ServerPeerMock peer = new ServerPeerMock();
            peer.setConfiguration(new ServerConfiguration(UUID.randomUUID(), "testEndpoint" + i));
            peers.add(peer);
            state.addPeer(peer);
        }
        electionProtocol.start();

        Times.setTest(4000);
        electionProtocol.onTimer(4000);
        assertThat(state.getRole(), is(ServerRole.FOLLOWER));

        Times.setTest(10000);
        electionProtocol.onTimer(10000);
        assertThat(state.getRole(), is(ServerRole.CANDIDATE));

        StateMachineTransactionMock transaction = ((StateMachineMock) context.getStateMachine()).getTransaction();
        assertThat(transaction.isCommitted(), is(true));

        Times.setTest(20000);
        electionProtocol.onTimer(20000);
        VoteResponseBuilder response1 = createVoteResponse(peers.get(0).getConfiguration().getId());
        electionProtocol.handleVoteResponse(response1.toMessage());

        VoteResponseBuilder response2 = createVoteResponse(peers.get(1).getConfiguration().getId());
        electionProtocol.handleVoteResponse(response2.toMessage());
        assertThat(state.getRole(), is(ServerRole.CANDIDATE));

        response1.setAccepted(true);
        response1.setSource(peers.get(2).getConfiguration().getId());
        electionProtocol.handleVoteResponse(response1.toMessage());
        electionProtocol.handleVoteResponse(response1.toMessage());
        assertThat(state.getRole(), is(ServerRole.CANDIDATE));

        response2.setAccepted(true);
        response2.setSource(peers.get(3).getConfiguration().getId());
        electionProtocol.handleVoteResponse(response2.toMessage());
        electionProtocol.handleVoteResponse(response2.toMessage());

        assertThat(state.getRole(), is(ServerRole.LEADER));
        assertThat(state.getLeader(), Is.is(context.getServerId()));
        assertThat(membershipProtocol.isOnLeader(), is(true));

        checkElectionTimer(false);

        Times.setTest(30000);
        electionProtocol.onTimer(30000);
        assertThat(state.getRole(), is(ServerRole.LEADER));

        assertThat(membershipProtocol.isOnLeader(), is(true));
    }

    @Test
    public void testBecomeFollower() throws Exception {
        state.setCatchingUp(false);

        List<ServerPeerMock> peers = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            ServerPeerMock peer = new ServerPeerMock();
            peer.setConfiguration(new ServerConfiguration(UUID.randomUUID(), "testEndpoint" + i));
            peers.add(peer);
            state.addPeer(peer);
        }

        electionProtocol.becomeFollower();
        assertThat(state.getRole(), is(ServerRole.FOLLOWER));
        checkElectionTimer(true);
        for (ServerPeerMock peer : peers)
            assertThat(peer.isHeartbeatEnabled(), is(false));

        assertThat(membershipProtocol.isOnFollower(), is(true));
    }

    @Test
    public void testElectionTimer() throws Exception {
        checkElectionTimer(false);
        state.setCatchingUp(true);

        electionProtocol.delayElection();
        checkElectionTimer(false);

        state.setCatchingUp(false);
        electionProtocol.delayElection();
        checkElectionTimer(true);
    }

    @Test
    public void testBlockElectionRequests() {
        state.setCatchingUp(false);
        electionProtocol.start();

        Times.setTest(2000);

        VoteRequestBuilder request = createVoteRequest(state.getConfiguration().getServers().get(1).getId());
        ServerResponse response = electionProtocol.handleVoteRequest(request.toMessage());
        assertThat(response.isAccepted(), is(false));

        Times.setTest(5000);
        request = createVoteRequest(state.getConfiguration().getServers().get(1).getId());
        response = electionProtocol.handleVoteRequest(request.toMessage());
        assertThat(response.isAccepted(), is(true));
    }

    @Test
    public void testHandleGreaterTermRequest() {
        state.setCatchingUp(false);
        state.setRole(ServerRole.CANDIDATE);
        electionProtocol.start();

        Times.setTest(5000);

        VoteRequestBuilder request = createVoteRequest(state.getConfiguration().getServers().get(1).getId());
        request.setTerm(3);
        ServerResponse response = electionProtocol.handleVoteRequest(request.toMessage());
        assertThat(response.isAccepted(), is(true));
        assertThat(state.getRole(), is(ServerRole.FOLLOWER));
        assertThat(state.getServerState().getTerm(), is(3L));
    }

    @Test
    public void testGrantVoteRequest() {
        state.setCatchingUp(false);
        state.getServerState().setTerm(2);
        electionProtocol.start();

        Times.setTest(5000);
        VoteRequestBuilder request = createVoteRequest(state.getConfiguration().getServers().get(1).getId());
        ServerResponse response = electionProtocol.handleVoteRequest(request.toMessage());
        assertThat(response.isAccepted(), is(false));

        UUID grantedId = state.getConfiguration().getServers().get(1).getId();
        request = createVoteRequest(grantedId);
        request.setTerm(2);
        response = electionProtocol.handleVoteRequest(request.toMessage());
        assertThat(response.isAccepted(), is(true));
        assertThat(state.getServerState().getVotedFor(), is(grantedId));
        StateMachineTransactionMock transaction = ((StateMachineMock) context.getStateMachine()).getTransaction();
        assertThat(transaction.isCommitted(), is(true));

        request = createVoteRequest(grantedId);
        request.setTerm(2);
        response = electionProtocol.handleVoteRequest(request.toMessage());
        assertThat(response.isAccepted(), is(true));
        assertThat(state.getServerState().getVotedFor(), is(grantedId));

        request = createVoteRequest(state.getConfiguration().getServers().get(2).getId());
        request.setTerm(2);
        response = electionProtocol.handleVoteRequest(request.toMessage());
        assertThat(response.isAccepted(), is(false));
        assertThat(state.getServerState().getVotedFor(), is(grantedId));
    }

    @Test
    public void testGrantVoteForNewerLogRequest() {
        state.setCatchingUp(false);
        state.getServerState().setTerm(2);
        electionProtocol.start();

        Times.setTest(5000);
        UUID grantedId = state.getConfiguration().getServers().get(1).getId();
        VoteRequestBuilder request = createVoteRequest(grantedId);
        request.setTerm(2);
        request.setLastLogTerm(2);
        ServerResponse response = electionProtocol.handleVoteRequest(request.toMessage());
        assertThat(response.isAccepted(), is(true));
        assertThat(state.getServerState().getVotedFor(), is(grantedId));

        request = createVoteRequest(grantedId);
        request.setTerm(2);
        request.setLastLogTerm(0);
        request.setLastLogIndex(2);
        response = electionProtocol.handleVoteRequest(request.toMessage());
        assertThat(response.isAccepted(), is(true));
        assertThat(state.getServerState().getVotedFor(), is(grantedId));
    }

    private VoteResponseBuilder createVoteResponse(UUID source) {
        VoteResponseBuilder response = new VoteResponseBuilder();
        response.setGroupId(state.getConfiguration().getGroupId());
        response.setSource(source);
        return response;
    }

    private VoteRequestBuilder createVoteRequest(UUID source) {
        VoteRequestBuilder request = new VoteRequestBuilder();
        request.setGroupId(state.getConfiguration().getGroupId());
        request.setSource(source);
        return request;
    }

    private void checkElectionTimer(boolean started) throws Exception {
        ICompartmentTimer timer = Tests.get(electionProtocol, "electionTimer");
        assertThat(timer.isStarted(), is(started));
        if (timer.isStarted()) {
            assertThat(timer.getPeriod() >= context.getServerChannelFactoryConfiguration().getMinElectionTimeout(), is(true));
            assertThat(timer.getPeriod() <= context.getServerChannelFactoryConfiguration().getMaxElectionTimeout(), is(true));

            long nextVotingTime = Tests.get(electionProtocol, "nextVotingTime");
            assertThat(nextVotingTime, Is.is(Times.getCurrentTime() + context.getServerChannelFactoryConfiguration().getMinElectionTimeout()));
        }
    }
}
