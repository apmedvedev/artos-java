package com.artos.impl.core.server.membership;

import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.api.core.server.conf.ServerConfiguration;
import com.artos.impl.core.message.FollowerResponse;
import com.artos.impl.core.message.LeaveGroupRequest;
import com.artos.impl.core.message.LeaveGroupRequestBuilder;
import com.artos.impl.core.message.LeaveGroupResponse;
import com.artos.impl.core.message.LeaveGroupResponseBuilder;
import com.artos.impl.core.message.MessageFlags;
import com.artos.impl.core.server.client.FollowerMessageSender;
import com.artos.impl.core.server.client.IFollowerMessageSender;
import com.artos.impl.core.server.election.IElectionProtocol;
import com.artos.impl.core.server.impl.Context;
import com.artos.impl.core.server.impl.IServerPeer;
import com.artos.impl.core.server.impl.ServerRole;
import com.artos.impl.core.server.impl.State;
import com.artos.impl.core.server.replication.IReplicationProtocol;
import com.artos.impl.core.server.utils.Utils;
import com.artos.spi.core.IStateMachineTransaction;
import com.artos.spi.core.LogEntry;
import com.artos.spi.core.LogValueType;
import com.exametrika.common.compartment.ICompartmentTimer;
import com.exametrika.common.compartment.impl.CompartmentTimer;
import com.exametrika.common.l10n.DefaultMessage;
import com.exametrika.common.l10n.ILocalizedMessage;
import com.exametrika.common.l10n.Messages;
import com.exametrika.common.l10n.SystemException;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.CompletionHandler;
import com.exametrika.common.utils.Enums;
import com.exametrika.common.utils.ICompletionHandler;
import com.exametrika.common.utils.Times;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class LeaveGroupProtocol implements ILeaveGroupProtocol {
    private static final IMessages messages = Messages.get(IMessages.class);
    private final Context context;
    private final State state;
    private IMembershipProtocol membershipProtocol;
    private IReplicationProtocol replicationProtocol;
    private IElectionProtocol electionProtocol;
    private final IFollowerMessageSender messageSender;
    private ICompartmentTimer leaveGroupTimer = new CompartmentTimer((currentTime) -> requestLeaveGroup());
    private ICompartmentTimer autoLeaveGroupTimeout = new CompartmentTimer((currentTime) -> handleAutoLeaveGroupTimeout());
    private ICompletionHandler leaveGroupCompletionHandler;

    public LeaveGroupProtocol(Context context, State state) {
        Assert.notNull(context);
        Assert.notNull(state);

        this.context = context;
        this.state = state;
        this.messageSender = new FollowerMessageSender(context, state, new CompletionHandler<FollowerResponse>() {
            @Override
            public void onSucceeded(FollowerResponse response) {
                handleLeaveGroupResponse((LeaveGroupResponse)response);
            }

            @Override
            public void onFailed(Throwable error) {
            }
        });
    }

    public void setElectionProtocol(IElectionProtocol electionProtocol) {
        Assert.notNull(electionProtocol);
        Assert.isNull(this.electionProtocol);

        this.electionProtocol = electionProtocol;
    }

    public void setReplicationProtocol(IReplicationProtocol replicationProtocol) {
        Assert.notNull(replicationProtocol);
        Assert.isNull(this.replicationProtocol);

        this.replicationProtocol = replicationProtocol;
    }

    public void setMembershipProtocol(IMembershipProtocol membershipProtocol) {
        Assert.notNull(membershipProtocol);
        Assert.isNull(this.membershipProtocol);

        this.membershipProtocol = membershipProtocol;
    }

    @Override
    public void start() {
        messageSender.start();
        leaveGroupTimer.setPeriod(context.getServerChannelFactoryConfiguration().getLeaveGroupPeriod());
        if (context.getServerChannelFactoryConfiguration().getAutoLeaveGroupTimeout() != 0)
            autoLeaveGroupTimeout.setPeriod(context.getServerChannelFactoryConfiguration().getAutoLeaveGroupTimeout());
    }

    @Override
    public void stop() {
        messageSender.stop();
    }

    @Override
    public void onTimer(long currentTime) {
        leaveGroupTimer.onTimer(currentTime);
        autoLeaveGroupTimeout.onTimer(currentTime);
    }

    @Override
    public LeaveGroupResponse handleLeaveGroupRequest(LeaveGroupRequest request) {
        LeaveGroupResponseBuilder response = new LeaveGroupResponseBuilder();
        response.setSource(context.getServerId());
        response.setGroupId(state.getConfiguration().getGroupId());
        response.setLeaderId(state.getLeader());

        if (state.getRole() != ServerRole.LEADER || state.isConfigurationChanging()) {
            response.setAccepted(false);
            return response.toMessage();
        }

        if (state.getConfiguration().findServer(request.getSource()) != null) {
            removeServersFromGroup(Collections.singleton(request.getSource()));
        } else {
            response.setFlags(Enums.of(MessageFlags.ALREADY_LEFT));
            response.setConfiguration(state.getConfiguration());
        }

        response.setAccepted(true);
        return response.toMessage();
    }

    @Override
    public void onLeader() {
        if (context.getServerChannelFactoryConfiguration().getAutoLeaveGroupTimeout() != 0)
            autoLeaveGroupTimeout.start();
    }

    @Override
    public void onFollower() {
        if (context.getServerChannelFactoryConfiguration().getAutoLeaveGroupTimeout() != 0)
            autoLeaveGroupTimeout.stop();
    }

    @Override
    public void gracefulClose(ICompletionHandler completionHandler) {
        if (state.getRole() == ServerRole.LEADER && !state.getPeers().isEmpty()) {
            electionProtocol.becomeFollower();
            electionProtocol.lockElection();
        }

        completionHandler.onSucceeded(null);
    }

    @Override
    public void leaveGroup(ICompletionHandler completionHandler) {
        if (state.getPeers().isEmpty()) {
            completionHandler.onFailed(new SystemException(messages.leaveGroupSingleServer()));
            return;
        }

        if (state.getRole() == ServerRole.LEADER) {
            electionProtocol.becomeFollower();
            electionProtocol.lockElection();
        }

        leaveGroupFollower(completionHandler);
    }

    @Override
    public void onLeft() {
        leaveGroupTimer.stop();
        if (leaveGroupCompletionHandler != null) {
            leaveGroupCompletionHandler.onSucceeded(null);
            leaveGroupCompletionHandler = null;
        }
    }

    private void leaveGroupFollower(ICompletionHandler completionHandler) {
        Assert.checkState(state.getRole() != ServerRole.LEADER);
        Assert.checkState(leaveGroupCompletionHandler == null);

        leaveGroupCompletionHandler = completionHandler;
        leaveGroupTimer.start();
    }

    private void requestLeaveGroup() {
        if (leaveGroupTimer.getAttempt() > context.getServerChannelFactoryConfiguration().getLeaveGroupAttemptCount()) {
            leaveGroupTimer.stop();
            leaveGroupCompletionHandler.onFailed(new SystemException(messages.leaveGroupTimeout()));
            leaveGroupCompletionHandler = null;

            return;
        }

        LeaveGroupRequestBuilder request = new LeaveGroupRequestBuilder();
        request.setSource(context.getServerId());
        request.setGroupId(state.getConfiguration().getGroupId());

        messageSender.send(request.toMessage());
    }

    private void handleLeaveGroupResponse(LeaveGroupResponse response) {
        if (!response.getFlags().contains(MessageFlags.ALREADY_LEFT) || response.getConfiguration() == null ||
            response.getConfiguration().findServer(context.getServerId()) != null)
            return;
        if (state.getConfiguration().findServer(context.getServerId()) == null)
            return;

        IStateMachineTransaction transaction = context.getStateMachine().beginTransaction(false);
        transaction.writeConfiguration(response.getConfiguration());
        transaction.commit();

        membershipProtocol.reconfigure(response.getConfiguration());
    }

    private void handleAutoLeaveGroupTimeout() {
        Assert.checkState(state.getRole() == ServerRole.LEADER);
        Assert.checkState(context.getServerChannelFactoryConfiguration().getAutoLeaveGroupTimeout() != 0);

        if (state.isConfigurationChanging())
            return;

        Set<UUID> leavingServers = new HashSet<>();
        long currentTime = Times.getCurrentTime();
        for (IServerPeer peer : state.getPeers().values()) {
            if (currentTime >= peer.getLastResponseTime() + context.getServerChannelFactoryConfiguration().getAutoLeaveGroupTimeout())
                leavingServers.add(peer.getConfiguration().getId());
        }

        if (!leavingServers.isEmpty())
            removeServersFromGroup(leavingServers);
    }

    private void removeServersFromGroup(Set<UUID> serverIds) {
        boolean changed = false;
        List<ServerConfiguration> servers = new ArrayList<>(state.getConfiguration().getServers());
        for (Iterator<ServerConfiguration> it = servers.iterator(); it.hasNext(); ) {
            ServerConfiguration configuration = it.next();
            if (serverIds.contains(configuration.getId())) {
                it.remove();
                changed = true;
            }
        }

        if (changed) {
            GroupConfiguration newConfiguration = new GroupConfiguration(state.getConfiguration().getName(),
                state.getConfiguration().getGroupId(), servers, state.getConfiguration().isSingleServer());

            context.getLogStore().append(new LogEntry(state.getServerState().getTerm(), Utils.writeGroupConfiguration(newConfiguration),
                LogValueType.CONFIGURATION, null, 0));

            membershipProtocol.reconfigure(newConfiguration);
            state.setConfigurationChanging(true);
            replicationProtocol.requestAppendEntries();
        }
    }

    private interface IMessages {
        @DefaultMessage("Leave group timeout occured.")
        ILocalizedMessage leaveGroupTimeout();

        @DefaultMessage("Could not leave group. Group consists of single server.")
        ILocalizedMessage leaveGroupSingleServer();
    }
}
