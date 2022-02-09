package com.artos.impl.core.server.membership;

import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.api.core.server.conf.ServerConfiguration;
import com.artos.impl.core.message.FollowerResponse;
import com.artos.impl.core.message.JoinGroupRequest;
import com.artos.impl.core.message.JoinGroupRequestBuilder;
import com.artos.impl.core.message.JoinGroupResponse;
import com.artos.impl.core.message.JoinGroupResponseBuilder;
import com.artos.impl.core.message.MessageFlags;
import com.artos.impl.core.server.client.FollowerMessageSender;
import com.artos.impl.core.server.client.IFollowerMessageSender;
import com.artos.impl.core.server.impl.Context;
import com.artos.impl.core.server.impl.ServerRole;
import com.artos.impl.core.server.impl.State;
import com.artos.impl.core.server.replication.IReplicationProtocol;
import com.artos.impl.core.server.state.IStateTransferProtocol;
import com.artos.impl.core.server.state.StateTransferConfiguration;
import com.artos.impl.core.server.utils.Utils;
import com.artos.spi.core.ILogStore;
import com.artos.spi.core.LogEntry;
import com.artos.spi.core.LogValueType;
import com.exametrika.common.compartment.ICompartmentTimer;
import com.exametrika.common.compartment.impl.CompartmentTimer;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.CompletionHandler;
import com.exametrika.common.utils.Enums;

import java.util.ArrayList;
import java.util.List;

public class JoinGroupProtocol implements IJoinGroupProtocol {
    private final Context context;
    private final State state;
    private IMembershipProtocol membershipProtocol;
    private IReplicationProtocol replicationProtocol;
    private IStateTransferProtocol stateTransferProtocol;
    private final IFollowerMessageSender messageSender;
    private ICompartmentTimer joinGroupTimer = new CompartmentTimer((currentTime) -> requestJoinGroup());
    private long rewindStep = 1;

    public JoinGroupProtocol(Context context, State state) {
        Assert.notNull(context);
        Assert.notNull(state);

        this.context = context;
        this.state = state;
        this.messageSender = new FollowerMessageSender(context, state, new CompletionHandler<FollowerResponse>() {
            @Override
            public void onSucceeded(FollowerResponse response) {
                handleJoinGroupResponse((JoinGroupResponse) response);
            }

            @Override
            public void onFailed(Throwable error) {
            }
        });
    }

    public void setMembershipProtocol(IMembershipProtocol membershipProtocol) {
        Assert.notNull(membershipProtocol);
        Assert.isNull(this.membershipProtocol);

        this.membershipProtocol = membershipProtocol;
    }

    public void setReplicationProtocol(IReplicationProtocol replicationProtocol) {
        Assert.notNull(replicationProtocol);
        Assert.isNull(this.replicationProtocol);

        this.replicationProtocol = replicationProtocol;
    }

    public void setStateTransferProtocol(IStateTransferProtocol stateTransferProtocol) {
        Assert.notNull(stateTransferProtocol);
        Assert.isNull(this.stateTransferProtocol);

        this.stateTransferProtocol = stateTransferProtocol;
    }

    @Override
    public void start() {
        messageSender.start();
        joinGroupTimer.setPeriod(context.getServerChannelFactoryConfiguration().getJoinGroupPeriod());
        joinGroupTimer.start();
    }

    @Override
    public void stop() {
        messageSender.stop();
    }

    @Override
    public void onTimer(long currentTime) {
        joinGroupTimer.onTimer(currentTime);
    }

    @Override
    public void onLeader() {
        joinGroupTimer.stop();
    }

    @Override
    public void onFollower() {
    }

    @Override
    public void onJoined() {
        joinGroupTimer.stop();
    }

    @Override
    public JoinGroupResponse handleJoinGroupRequest(JoinGroupRequest request) {
        JoinGroupResponseBuilder response = new JoinGroupResponseBuilder();
        response.setSource(context.getServerId());
        response.setGroupId(state.getConfiguration().getGroupId());
        response.setLeaderId(state.getLeader());

        if (state.getRole() != ServerRole.LEADER || state.isConfigurationChanging()) {
            response.setAccepted(false);
            return response.toMessage();
        }

        if (state.getConfiguration().findServer(request.getSource()) != null) {
            response.setFlags(Enums.of(MessageFlags.ALREADY_JOINED));
        } else {
            long startIndex = context.getLogStore().getStartIndex();
            long currentNextIndex = context.getLogStore().getEndIndex();
            long lastLogIndex = request.getLastLogIndex();

            if (startIndex > 1 && lastLogIndex < startIndex) {
                response.setFlags(Enums.of(MessageFlags.CATCHING_UP, MessageFlags.STATE_TRANSFER_REQUIRED));

                String host = stateTransferProtocol.getHost();
                int port = stateTransferProtocol.getPort();
                long startLogIndex = 1;

                StateTransferConfiguration stateTransferConfiguration = new StateTransferConfiguration(host, port, startLogIndex);
                response.setStateTransferConfiguration(stateTransferConfiguration);
            } else {
                boolean logOk = request.getLastLogIndex() == 0 || (request.getLastLogIndex() < context.getLogStore().getEndIndex() &&
                    request.getLastLogTerm() == getTermForLogIndex(context.getLogStore(), request.getLastLogIndex()));

                if (logOk) {
                    if (currentNextIndex - 1 - lastLogIndex >= context.getServerChannelFactoryConfiguration().getMinStateTransferGapLogEntryCount()) {
                        response.setFlags(Enums.of(MessageFlags.CATCHING_UP, MessageFlags.STATE_TRANSFER_REQUIRED));

                        String host = stateTransferProtocol.getHost();
                        int port = stateTransferProtocol.getPort();
                        long startLogIndex = lastLogIndex + 1;

                        StateTransferConfiguration stateTransferConfiguration = new StateTransferConfiguration(host, port, startLogIndex);
                        response.setStateTransferConfiguration(stateTransferConfiguration);
                    } else {
                        List<ServerConfiguration> servers = new ArrayList<>(state.getConfiguration().getServers());
                        servers.add(request.getConfiguration());

                        GroupConfiguration newConfiguration = new GroupConfiguration(state.getConfiguration().getName(),
                            state.getConfiguration().getGroupId(), servers, state.getConfiguration().isSingleServer());

                        context.getLogStore().append(new LogEntry(state.getServerState().getTerm(), Utils.writeGroupConfiguration(newConfiguration),
                            LogValueType.CONFIGURATION, null, 0));

                        membershipProtocol.reconfigure(newConfiguration);
                        state.setConfigurationChanging(true);
                        replicationProtocol.requestAppendEntries();

                        response.setFlags(Enums.of(MessageFlags.CATCHING_UP));
                    }
                } else {
                    response.setFlags(Enums.of(MessageFlags.CATCHING_UP, MessageFlags.REWIND_REPEAT_JOIN));
                }
            }
        }

        response.setAccepted(true);
        return response.toMessage();
    }

    private void requestJoinGroup() {
        JoinGroupRequestBuilder request = new JoinGroupRequestBuilder();
        request.setSource(context.getServerId());
        request.setGroupId(state.getConfiguration().getGroupId());

        long logIndex = context.getLogStore().getEndIndex() - rewindStep;
        if (logIndex >= context.getLogStore().getStartIndex()) {
            LogEntry logEntry = context.getLogStore().getAt(logIndex);
            request.setLastLogTerm(logEntry.getTerm());
            request.setLastLogIndex(logIndex);
        }

        request.setConfiguration(context.getLocalServer());

        messageSender.send(request.toMessage());
    }

    private void handleJoinGroupResponse(JoinGroupResponse response) {
        Assert.isTrue(response.isAccepted());

        long rewindStep = this.rewindStep;
        this.rewindStep = 1;

        if (response.getFlags().contains(MessageFlags.ALREADY_JOINED)) {
            state.setCatchingUp(false);
            joinGroupTimer.stop();
        } else {
            if (response.getFlags().contains(MessageFlags.CATCHING_UP))
                state.setCatchingUp(true);

            if (response.getFlags().contains(MessageFlags.STATE_TRANSFER_REQUIRED)) {
                joinGroupTimer.stop();
                StateTransferConfiguration stateTransferConfiguration = response.getStateTransferConfiguration();
                Assert.notNull(stateTransferConfiguration);

                stateTransferProtocol.requestStateTransfer(stateTransferConfiguration.getHost(), stateTransferConfiguration.getPort(),
                    stateTransferConfiguration.getStartLogIndex(), new CompletionHandler() {
                    @Override
                    public void onSucceeded(Object result) {
                        joinGroupTimer.start();
                    }

                    @Override
                    public void onFailed(Throwable error) {
                        joinGroupTimer.start();
                    }
                });
            }

            if (response.getFlags().contains(MessageFlags.REWIND_REPEAT_JOIN))
                this.rewindStep = rewindStep * 2;
        }
    }

    private static long getTermForLogIndex(ILogStore logStore, long logIndex) {
        if (logIndex == 0)
            return 0;

        if (logIndex >= logStore.getStartIndex())
            return logStore.getAt(logIndex).getTerm();
        else
            return -1;
    }
}
