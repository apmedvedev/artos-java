package com.artos.tests.core;

import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.api.core.server.conf.ServerConfiguration;
import com.artos.impl.core.server.impl.ServerPeer;
import com.artos.impl.core.server.impl.ServerRole;
import com.artos.impl.core.server.membership.MembershipProtocol;
import com.artos.impl.core.server.membership.MembershipService;
import com.artos.tests.core.mocks.JoinGroupProtocolMock;
import com.artos.tests.core.mocks.LeaveGroupProtocolMock;
import com.artos.tests.core.mocks.ServerPeerMock;
import com.exametrika.common.tests.Tests;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

public class MembershipProtocolTests extends AbstractRaftTest {
    private MembershipProtocol membershipProtocol;
    private JoinGroupProtocolMock joinGroupProtocol;
    private LeaveGroupProtocolMock leaveGroupProtocol;
    private MembershipService membershipService;

    @Before
    public void setUp() {
        createState(10);
        context = createContext(state.getConfiguration());

        joinGroupProtocol = new JoinGroupProtocolMock();
        leaveGroupProtocol = new LeaveGroupProtocolMock();

        membershipService = new MembershipService(state);
        membershipService.setContext(context);

        membershipProtocol = new MembershipProtocol(null, context, state);
        membershipProtocol.setJoinGroupProtocol(joinGroupProtocol);
        membershipProtocol.setLeaveGroupProtocol(leaveGroupProtocol);
        membershipProtocol.setMembershipService(membershipService);

        List<ServerPeerMock> peers = new ArrayList<>();
        for (int i = 0; i < state.getConfiguration().getServers().size(); i++) {
            ServerConfiguration configuration = state.getConfiguration().getServers().get(i);
            if (!configuration.getId().equals(context.getServerId())) {
                ServerPeerMock peer = new ServerPeerMock();
                peer.setConfiguration(new ServerConfiguration(configuration.getId(), configuration.getEndpoint()));
                peer.start();
                peer.setNextLogIndex(11);
                peers.add(peer);
                state.addPeer(peer);
            }
        }
    }

    @Test
    public void testReconfigureNew() throws Throwable {
        state.setRole(ServerRole.LEADER);
        addLogEntries(UUID.randomUUID(), 2, 10);

        GroupConfiguration oldConfiguration = state.getConfiguration();
        List<ServerConfiguration> servers = new ArrayList<>(oldConfiguration.getServers());
        servers.remove(oldConfiguration.findServer(context.getServerId()));
        oldConfiguration = new GroupConfiguration(oldConfiguration.getName(), oldConfiguration.getGroupId(), servers, false);
        state.setConfiguration(oldConfiguration);

        ServerConfiguration newServer = new ServerConfiguration(UUID.randomUUID(), "testNewServer");
        ServerConfiguration newSelfServer = new ServerConfiguration(context.getServerId(), "testSelfServer");
        servers = new ArrayList<>(state.getConfiguration().getServers());
        servers.add(newServer);
        servers.add(newSelfServer);

        GroupConfiguration newConfiguration = new GroupConfiguration(state.getConfiguration().getName(), state.getConfiguration().getGroupId(), servers, false);
        membershipProtocol.reconfigure(newConfiguration);

        assertThat(state.getConfiguration() == newConfiguration, is(true));
        assertThat(newConfiguration.getGroupId(), is(oldConfiguration.getGroupId()));
        assertThat(newConfiguration.getServers().size(), is(oldConfiguration.getServers().size() + 2));
        assertThat(newConfiguration.getServers().size(), is(state.getPeers().size() + 1));
        for (ServerConfiguration configuration : oldConfiguration.getServers()) {
            assertThat(newConfiguration.findServer(configuration.getId()), is(configuration));
            ServerPeerMock peer = (ServerPeerMock) state.getPeers().get(configuration.getId());
            assertThat(peer.getConfiguration(), is(configuration));
            assertThat(peer.isHeartbeatEnabled(), is(true));
            assertThat(peer.getNextLogIndex(), is(11L));
        }

        assertThat(newConfiguration.findServer(newServer.getId()), is(newServer));
        ServerPeer peer = (ServerPeer) state.getPeers().get(newServer.getId());
        assertThat(peer.getConfiguration(), is(newServer));
        assertThat(Tests.get(peer, "heartbeatEnabled"), is(true));
        assertThat(peer.getNextLogIndex(), is(11L));
    }

    @Test
    public void testCommitConfiguration() {
        state.setConfigurationChanging(true);
        GroupConfiguration oldConfiguration = state.getConfiguration();

        List<ServerConfiguration> servers = new ArrayList<>(oldConfiguration.getServers());
        servers.remove(oldConfiguration.findServer(context.getServerId()));
        servers.remove(oldConfiguration.findServer(oldConfiguration.getServers().get(1).getId()));

        ServerPeerMock oldPeer = (ServerPeerMock) state.getPeers().get(oldConfiguration.getServers().get(1).getId());

        GroupConfiguration newConfiguration = new GroupConfiguration(state.getConfiguration().getName(),
            state.getConfiguration().getGroupId(), servers, false);
        membershipProtocol.commitConfiguration(newConfiguration);
        assertThat(state.isConfigurationChanging(), is(false));

        assertThat(state.getConfiguration() == newConfiguration, is(true));
        assertThat(newConfiguration.getGroupId(), is(oldConfiguration.getGroupId()));
        assertThat(newConfiguration.getServers().size(), is(oldConfiguration.getServers().size() - 2));
        assertThat(newConfiguration.getServers().size(), is(state.getPeers().size()));
        for (ServerConfiguration configuration : newConfiguration.getServers()) {
            assertThat(newConfiguration.findServer(configuration.getId()), is(configuration));
            ServerPeerMock peer = (ServerPeerMock) state.getPeers().get(configuration.getId());
            assertThat(peer.getConfiguration(), is(configuration));
            assertThat(peer.isHeartbeatEnabled(), is(true));
            assertThat(peer.getNextLogIndex(), is(11L));
        }

        assertThat(state.getPeers().get(oldPeer.getConfiguration().getId()), nullValue());
        assertThat(oldPeer.isHeartbeatEnabled(), is(false));

        assertThat(leaveGroupProtocol.isOnLeft(), is(true));
    }

    @Test
    public void testCommitConfigurationJoined() throws Throwable {
        state.setConfigurationChanging(true);
        state.setCatchingUp(true);

        state.setRole(ServerRole.LEADER);
        addLogEntries(UUID.randomUUID(), 2, 10);

        GroupConfiguration oldConfiguration = state.getConfiguration();
        List<ServerConfiguration> servers = new ArrayList<>(oldConfiguration.getServers());
        servers.remove(oldConfiguration.findServer(context.getServerId()));
        oldConfiguration = new GroupConfiguration(oldConfiguration.getName(), oldConfiguration.getGroupId(), servers, false);
        state.setConfiguration(oldConfiguration);

        ServerConfiguration newServer = new ServerConfiguration(UUID.randomUUID(), "testNewServer");
        ServerConfiguration newSelfServer = new ServerConfiguration(context.getServerId(), "testSelfServer");
        servers = new ArrayList<>(state.getConfiguration().getServers());
        servers.add(newServer);
        servers.add(newSelfServer);

        GroupConfiguration newConfiguration = new GroupConfiguration(state.getConfiguration().getName(),
            state.getConfiguration().getGroupId(), servers, false);
        membershipProtocol.commitConfiguration(newConfiguration);
        assertThat(state.isConfigurationChanging(), is(false));

        assertThat(state.getConfiguration() == newConfiguration, is(true));
        assertThat(newConfiguration.getGroupId(), is(oldConfiguration.getGroupId()));
        assertThat(newConfiguration.getServers().size(), is(oldConfiguration.getServers().size() + 2));
        assertThat(newConfiguration.getServers().size(), is(state.getPeers().size() + 1));
        for (ServerConfiguration configuration : oldConfiguration.getServers()) {
            assertThat(newConfiguration.findServer(configuration.getId()), is(configuration));
            ServerPeerMock peer = (ServerPeerMock) state.getPeers().get(configuration.getId());
            assertThat(peer.getConfiguration(), is(configuration));
            assertThat(peer.isHeartbeatEnabled(), is(true));
            assertThat(peer.getNextLogIndex(), is(11L));
        }

        assertThat(newConfiguration.findServer(newServer.getId()), is(newServer));
        ServerPeer peer = (ServerPeer) state.getPeers().get(newServer.getId());
        assertThat(peer.getConfiguration(), is(newServer));
        assertThat(Tests.get(peer, "heartbeatEnabled"), is(true));
        assertThat(peer.getNextLogIndex(), is(11L));

        assertThat(joinGroupProtocol.isOnJoined(), is(true));
    }
}
