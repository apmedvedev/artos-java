package com.artos.impl.core.server.membership;

import com.artos.api.core.server.MembershipChange;
import com.artos.api.core.server.MembershipChangeEvent;
import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.api.core.server.conf.ServerConfiguration;
import com.artos.impl.core.server.client.ILeaderClientProtocol;
import com.artos.impl.core.server.impl.Context;
import com.artos.impl.core.server.impl.IServer;
import com.artos.impl.core.server.impl.IServerPeer;
import com.artos.impl.core.server.impl.ServerRole;
import com.artos.impl.core.server.impl.State;
import com.artos.impl.core.server.replication.IQueryProtocol;
import com.artos.impl.core.server.replication.IReplicationProtocol;
import com.artos.impl.core.server.state.IStateTransferProtocol;
import com.exametrika.common.l10n.DefaultMessage;
import com.exametrika.common.l10n.ILocalizedMessage;
import com.exametrika.common.l10n.Messages;
import com.exametrika.common.log.ILogger;
import com.exametrika.common.log.IMarker;
import com.exametrika.common.log.LogLevel;
import com.exametrika.common.log.Loggers;
import com.exametrika.common.utils.Assert;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class MembershipProtocol implements IMembershipProtocol {
    private static final IMessages messages = Messages.get(IMessages.class);
    private final ILogger logger = Loggers.get(getClass());
    private final IMarker marker;
    private final IServer server;
    private final Context context;
    private final State state;
    private IReplicationProtocol replicationProtocol;
    private IJoinGroupProtocol joinGroupProtocol;
    private ILeaveGroupProtocol leaveGroupProtocol;
    private IStateTransferProtocol stateTransferProtocol;
    private ILeaderClientProtocol leaderClientProtocol;
    private IQueryProtocol queryProtocol;
    private MembershipService membershipService;

    public MembershipProtocol(IServer server, Context context, State state) {
        Assert.notNull(server);
        Assert.notNull(context);
        Assert.notNull(state);

        this.server = server;
        this.context = context;
        this.state = state;
        this.marker = context.getMarker();
    }

    public void setReplicationProtocol(IReplicationProtocol replicationProtocol) {
        Assert.notNull(replicationProtocol);
        Assert.isNull(this.replicationProtocol);

        this.replicationProtocol = replicationProtocol;
    }

    public void setLeaveGroupProtocol(ILeaveGroupProtocol leaveGroupProtocol) {
        Assert.notNull(leaveGroupProtocol);
        Assert.isNull(this.leaveGroupProtocol);

        this.leaveGroupProtocol = leaveGroupProtocol;
    }

    public void setJoinGroupProtocol(IJoinGroupProtocol joinGroupProtocol) {
        Assert.notNull(joinGroupProtocol);
        Assert.isNull(this.joinGroupProtocol);

        this.joinGroupProtocol = joinGroupProtocol;
    }

    public void setStateTransferProtocol(IStateTransferProtocol stateTransferProtocol) {
        Assert.notNull(stateTransferProtocol);
        Assert.isNull(this.stateTransferProtocol);

        this.stateTransferProtocol = stateTransferProtocol;
    }

    public void setLeaderClientProtocol(ILeaderClientProtocol leaderClientProtocol) {
        Assert.notNull(leaderClientProtocol);
        Assert.isNull(this.leaderClientProtocol);

        this.leaderClientProtocol = leaderClientProtocol;
    }

    public void setQueryProtocol(IQueryProtocol queryProtocol) {
        Assert.notNull(queryProtocol);
        Assert.isNull(this.queryProtocol);

        this.queryProtocol = queryProtocol;
    }

    public void setMembershipService(MembershipService membershipService) {
        Assert.notNull(membershipService);
        Assert.isNull(this.membershipService);

        this.membershipService = membershipService;
    }

    @Override
    public void onLeader() {
        replicationProtocol.onLeader();
        leaveGroupProtocol.onLeader();
        joinGroupProtocol.onLeader();
        stateTransferProtocol.onLeader();
        membershipService.onLeader();
        leaderClientProtocol.onLeader();
    }

    @Override
    public void onFollower() {
        replicationProtocol.onFollower();
        leaveGroupProtocol.onFollower();
        joinGroupProtocol.onFollower();
        stateTransferProtocol.onFollower();
        membershipService.onFollower();
        leaderClientProtocol.onFollower();
        queryProtocol.onFollower();
    }

    @Override
    public void reconfigure(GroupConfiguration newConfiguration) {
        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.configurationChanged(state.getConfiguration(), newConfiguration));

        state.setConfigurationChanging(false);

        GroupConfiguration oldConfiguration = state.getConfiguration();
        Assert.checkState(oldConfiguration != null);
        Assert.checkState(newConfiguration.getGroupId().equals(oldConfiguration.getGroupId()));

        List<UUID> removedServers = new LinkedList<>();
        for (ServerConfiguration server : oldConfiguration.getServers()) {
            if (newConfiguration.findServer(server.getId()) == null)
                removedServers.add(server.getId());
        }

        List<ServerConfiguration> addedServers = new LinkedList<>();
        for (ServerConfiguration server : newConfiguration.getServers()) {
            if (oldConfiguration.findServer(server.getId()) == null)
                addedServers.add(server);
        }

        for (ServerConfiguration serverConfiguration : addedServers) {
             if (serverConfiguration.getId().equals(context.getServerId()))
                 continue;

            IServerPeer peer = server.createPeer(serverConfiguration);
            peer.setNextLogIndex(context.getLogStore().getEndIndex());
            state.addPeer(peer);

            if (logger.isLogEnabled(LogLevel.INFO))
                logger.log(LogLevel.INFO, marker, messages.serverAdded(serverConfiguration));

            if (state.getRole() == ServerRole.LEADER)
                peer.start();
        }

        for (UUID id : removedServers) {
            if (id.equals(context.getServerId()) && !state.isCatchingUp()) {
                if (logger.isLogEnabled(LogLevel.INFO))
                    logger.log(LogLevel.INFO, marker, messages.currentServerLeft());

                onLeft();
            } else {
                IServerPeer peer = state.getPeers().get(id);
                if (peer != null) {
                    peer.stop();
                    state.removePeer(id);

                    if (logger.isLogEnabled(LogLevel.INFO))
                        logger.log(LogLevel.INFO, marker, messages.serverRemoved(peer.getConfiguration()));
                }
            }
        }

        state.setConfiguration(newConfiguration);

        MembershipChange change = new MembershipChange(addedServers, removedServers);
        MembershipChangeEvent event = new MembershipChangeEvent(oldConfiguration, newConfiguration, change);
        onMembershipChanged(event);
    }

    @Override
    public void commitConfiguration(GroupConfiguration newConfiguration) {
        reconfigure(newConfiguration);

        if (state.isCatchingUp() && newConfiguration.findServer(context.getServerId()) != null) {
            if (logger.isLogEnabled(LogLevel.INFO))
                logger.log(LogLevel.INFO, marker, messages.currentServerJoined());

            state.setCatchingUp(false);
            onJoined();
        }
    }

    private void onJoined() {
        joinGroupProtocol.onJoined();
        membershipService.onJoined();
    }

    private void onLeft() {
        leaveGroupProtocol.onLeft();
        membershipService.onLeft();
        replicationProtocol.onLeft();
    }

    private void onMembershipChanged(MembershipChangeEvent event) {
        membershipService.onMembershipChanged(event);
    }

    private interface IMessages {
        @DefaultMessage("Configuration is changed. \nOld configuration: {0}\nNew configuration: {1}")
        ILocalizedMessage configurationChanged(GroupConfiguration oldConfiguration, GroupConfiguration newConfiguration);

        @DefaultMessage("Server ''{0}'' has been added to group.")
        ILocalizedMessage serverAdded(ServerConfiguration configuration);

        @DefaultMessage("Server ''{0}'' has been removed from group.")
        ILocalizedMessage serverRemoved(ServerConfiguration configuration);

        @DefaultMessage("Current server has joined the group.")
        ILocalizedMessage currentServerJoined();

        @DefaultMessage("Current server has left the group.")
        ILocalizedMessage currentServerLeft();
    }
}
