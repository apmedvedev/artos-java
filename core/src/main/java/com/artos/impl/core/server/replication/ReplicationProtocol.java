/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.server.replication;

import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.api.core.server.conf.ServerConfiguration;
import com.artos.impl.core.message.AppendEntriesRequest;
import com.artos.impl.core.message.AppendEntriesRequestBuilder;
import com.artos.impl.core.message.AppendEntriesResponse;
import com.artos.impl.core.message.AppendEntriesResponseBuilder;
import com.artos.impl.core.message.ClientSessionTransientState;
import com.artos.impl.core.message.GroupTransientState;
import com.artos.impl.core.message.GroupTransientStateBuilder;
import com.artos.impl.core.message.MessageFlags;
import com.artos.impl.core.message.PublishRequest;
import com.artos.impl.core.message.PublishResponse;
import com.artos.impl.core.message.PublishResponseBuilder;
import com.artos.impl.core.server.election.IElectionProtocol;
import com.artos.impl.core.server.impl.Context;
import com.artos.impl.core.server.impl.IServer;
import com.artos.impl.core.server.impl.IServerPeer;
import com.artos.impl.core.server.impl.ServerRole;
import com.artos.impl.core.server.impl.State;
import com.artos.impl.core.server.membership.IMembershipProtocol;
import com.artos.impl.core.server.state.IStateTransferProtocol;
import com.artos.impl.core.server.state.StateTransferConfiguration;
import com.artos.impl.core.server.utils.Utils;
import com.artos.spi.core.ILogStore;
import com.artos.spi.core.IStateMachineTransaction;
import com.artos.spi.core.LogEntry;
import com.artos.spi.core.LogValueType;
import com.artos.spi.core.ServerState;
import com.exametrika.common.l10n.DefaultMessage;
import com.exametrika.common.l10n.ILocalizedMessage;
import com.exametrika.common.l10n.Messages;
import com.exametrika.common.log.ILogger;
import com.exametrika.common.log.IMarker;
import com.exametrika.common.log.LogLevel;
import com.exametrika.common.log.Loggers;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.CompletionHandler;
import com.exametrika.common.utils.Enums;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

public class ReplicationProtocol implements IReplicationProtocol {
    private static final IMessages messages = Messages.get(IMessages.class);
    private final ILogger logger = Loggers.get(getClass());
    private final IMarker marker;
    private final IServer server;
    private final Context context;
    private final State state;
    private final ClientSessionManager clientSessionManager;
    private IElectionProtocol electionProtocol;
    private IMembershipProtocol membershipProtocol;
    private IStateTransferProtocol stateTransferProtocol;
    private IQueryProtocol queryProtocol;
    private boolean commitsLocked;
    private boolean stateTransferInProgress;

    public ReplicationProtocol(IServer server, Context context, State state) {
        Assert.notNull(server);
        Assert.notNull(context);
        Assert.notNull(state);

        this.server = server;
        this.context = context;
        this.state = state;
        this.marker = context.getMarker();
        this.clientSessionManager = new ClientSessionManager(this, context.getServerChannelFactoryConfiguration().getClientSessionTimeout());
    }

    public void setElectionProtocol(IElectionProtocol electionProtocol) {
        Assert.notNull(electionProtocol);
        Assert.isNull(this.electionProtocol);

        this.electionProtocol = electionProtocol;
    }

    public void setMembershipProtocol(IMembershipProtocol membershipProtocol) {
        Assert.notNull(membershipProtocol);
        Assert.isNull(this.membershipProtocol);

        this.membershipProtocol = membershipProtocol;
    }

    public void setStateTransferProtocol(IStateTransferProtocol stateTransferProtocol) {
        Assert.notNull(stateTransferProtocol);
        Assert.isNull(this.stateTransferProtocol);

        this.stateTransferProtocol = stateTransferProtocol;
    }

    public void setQueryProtocol(IQueryProtocol queryProtocol) {
        Assert.notNull(queryProtocol);
        Assert.isNull(this.queryProtocol);

        this.queryProtocol = queryProtocol;
    }

    @Override
    public void start() {
        IStateMachineTransaction transaction = context.getStateMachine().beginTransaction(true);
        state.setServerState(transaction.readState());

        if (state.getServerState() == null)
            state.setServerState(new ServerState());

        GroupConfiguration configuration = transaction.readConfiguration();
        transaction.commit();

        if (configuration != null)
            state.setConfiguration(configuration);

        Assert.checkState(state.getConfiguration() != null);
        Assert.checkState(state.getConfiguration().getGroupId().equals(context.getGroupId()));

        if (state.getConfiguration().findServer(context.getServerId()) == null)
            state.setCatchingUp(true);

        for (ServerConfiguration serverConfiguration : state.getConfiguration().getServers()) {
            if (!serverConfiguration.getId().equals(context.getServerId()))
                state.addPeer(server.createPeer(serverConfiguration));
        }

        state.setQuickCommitIndex(state.getServerState().getCommitIndex());

        if (context.getLogStore().getEndIndex() - 1 < state.getServerState().getCommitIndex())
            context.getLogStore().clear(state.getServerState().getCommitIndex());
    }

    @Override
    public void stop() {
    }

    @Override
    public void onTimer(long currentTime) {
        if (state.getRole() == ServerRole.LEADER) {
            int disconnectedCount = 0;
            for (IServerPeer peer : state.getPeers().values()) {
                if (peer.canHeartbeat(currentTime))
                    requestAppendEntries(peer);
                else if (!peer.isConnected())
                    disconnectedCount++;
            }

            if (disconnectedCount > (state.getPeers().size() + 1) / 2)
                electionProtocol.becomeFollower();
        }

        clientSessionManager.onTimer(currentTime);
    }

    @Override
    public ClientSessionManager getClientSessionManager() {
        return clientSessionManager;
    }

    @Override
    public void onLeader() {
        Assert.checkState(state.getRole() == ServerRole.LEADER);

        if (state.getQuickCommitIndex() == 0)
            addInitialConfiguration();

        for (IServerPeer peer : state.getPeers().values()) {
            peer.setNextLogIndex(context.getLogStore().getEndIndex());
            peer.start();
        }

        long logIndex = state.getQuickCommitIndex() + 1;
        while (logIndex < context.getLogStore().getEndIndex()) {
            LogEntry logEntry = context.getLogStore().getAt(logIndex);
            if (logEntry.getValueType() == LogValueType.CONFIGURATION) {
                if (logger.isLogEnabled(LogLevel.DEBUG))
                    logger.log(LogLevel.DEBUG, marker, messages.uncommittedConfigurationChange(logIndex));

                GroupConfiguration configuration = Utils.readGroupConfiguration(logEntry.getValue());
                membershipProtocol.reconfigure(configuration);
                state.setConfigurationChanging(true);
            }

            logIndex++;
        }

        requestAppendEntries();
    }

    @Override
    public void onFollower() {
        Assert.checkState(state.getRole() == ServerRole.FOLLOWER);

        for (IServerPeer peer : state.getPeers().values())
            peer.stop();
    }

    @Override
    public void onLeft() {
        clientSessionManager.clear();
    }

    @Override
    public long publish(LogEntry logEntry) {
        if (state.getRole() != ServerRole.LEADER)
            return 0;

        long term = state.getServerState().getTerm();

        ClientSession session = clientSessionManager.ensureSession(context.getServerId());
        if (logEntry != null) {
            boolean received = session.receive(logEntry.getMessageId());
            Assert.checkState(received);

            context.getLogStore().append(new LogEntry(term, logEntry.getValue(), logEntry.getValueType(),
                context.getServerId(), logEntry.getMessageId()));

            requestAppendEntries();
        }

        return session.getLastCommittedMessageId();
    }

    @Override
    public long getLastReceivedMessageId() {
        ClientSession session = clientSessionManager.ensureSession(context.getServerId());
        return session.getLastReceivedMessageId();
    }

    @Override
    public void requestAppendEntries() {
        if (state.getPeers().isEmpty()) {
            commit(context.getLogStore().getEndIndex() - 1);
            return;
        }

        for (IServerPeer peer : state.getPeers().values()) {
            requestAppendEntries(peer);
        }
    }

    @Override
    public void lockCommits() {
        commitsLocked = true;
    }

    @Override
    public void unlockCommits() {
        commitsLocked = false;
    }

    @Override
    public void acquireTransientState(GroupTransientStateBuilder transientState) {
        transientState.setCommitIndex(state.getQuickCommitIndex());

        List<ClientSessionTransientState> clientSessions = new ArrayList<>();
        for (ClientSession session : clientSessionManager.getSessions().values())
            clientSessions.add(new ClientSessionTransientState(session.getClientId(), session.getLastCommittedMessageId()));

        transientState.setClientSessions(clientSessions);
    }

    @Override
    public void applyTransientState(Collection<GroupTransientState> transientStates) {
        Assert.checkState(state.getRole() == ServerRole.LEADER);

        long minCommitIndex = state.getQuickCommitIndex();
        for (GroupTransientState transientState : transientStates) {
            if (minCommitIndex > transientState.getCommitIndex())
                minCommitIndex = transientState.getCommitIndex();

            for (ClientSessionTransientState clientSessionState : transientState.getClientSessions()) {
                ClientSession session = clientSessionManager.ensureSession(clientSessionState.getClientId());
                if (session.getLastCommittedMessageId() < clientSessionState.getLastCommittedMessageId()) {
                    session.commit(clientSessionState.getLastCommittedMessageId());
                    session.setCommitted(false);
                }
            }
        }

        long logIndex = Math.max(context.getLogStore().getStartIndex(), minCommitIndex + 1);
        while (logIndex < context.getLogStore().getEndIndex()) {
            LogEntry logEntry = context.getLogStore().getAt(logIndex);
            if (logEntry.getClientId() != null) {
                ClientSession session = clientSessionManager.ensureSession(logEntry.getClientId());
                session.setLastReceivedMessageId(logEntry.getMessageId());
            }

            logIndex++;
        }
    }

    @Override
    public PublishResponse handlePublishRequest(PublishRequest request) {
        PublishResponseBuilder response = new PublishResponseBuilder();
        response.setSource(context.getServerId());
        response.setGroupId(state.getConfiguration().getGroupId());
        response.setLeaderId(state.getLeader());

        if (state.getRole() != ServerRole.LEADER) {
            response.setAccepted(false);
            response.setConfiguration(state.getConfiguration());
            return response.toMessage();
        }

        long term = state.getServerState().getTerm();

        if (request.getFlags().contains(MessageFlags.REQUEST_CONFIGURATION))
            response.setConfiguration(state.getConfiguration());

        ClientSession session = clientSessionManager.ensureSession(request.getSource());
        if (request.getFlags().contains(MessageFlags.RESTART_SESSION)) {
            List<LogEntry> logEntries = request.getLogEntries();
            Assert.notNull(logEntries);
            Assert.checkState(logEntries.size() == 1);

            LogEntry logEntry = logEntries.get(0);
            session.setLastReceivedMessageId(logEntry.getMessageId());
            session.setOutOfOrderReceived(false);
        } else {
            List<LogEntry> logEntries = request.getLogEntries();
            if (logEntries != null && !logEntries.isEmpty()) {
                boolean appended = false;
                for (LogEntry logEntry : logEntries) {
                    if (session.receive(logEntry.getMessageId())) {
                        context.getLogStore().append(new LogEntry(term, logEntry.getValue(), logEntry.getValueType(),
                            request.getSource(), logEntry.getMessageId()));
                        appended = true;
                    }
                }

                if (appended)
                    requestAppendEntries();
            }
        }

        response.setAccepted(true);
        response.setLastReceivedMessageId(session.getLastReceivedMessageId());
        response.setLastCommittedMessageId(session.getLastCommittedMessageId());

        if (session.isOutOfOrderReceived())
            response.setFlags(Enums.of(MessageFlags.OUT_OF_ORDER_RECEIVED));

        return response.toMessage();
    }

    @Override
    public AppendEntriesResponse handleAppendEntriesRequest(AppendEntriesRequest request) {
        AppendEntriesResponseBuilder response = new AppendEntriesResponseBuilder();
        response.setSource(context.getServerId());
        response.setGroupId(state.getConfiguration().getGroupId());

        if (state.getConfiguration().findServer(request.getSource()) == null) {
            response.setAccepted(false);
            return response.toMessage();
        }

        Utils.updateTerm(context, electionProtocol, state, request.getTerm());

        if (request.getTerm() == state.getServerState().getTerm()) {
            if (state.getRole() == ServerRole.CANDIDATE)
                electionProtocol.becomeFollower();
            else {
                Assert.checkState(state.getRole() != ServerRole.LEADER);
                electionProtocol.delayElection();
            }
        }

        response.setTerm(state.getServerState().getTerm());

        boolean logOk = request.getLastLogIndex() == 0 || (request.getLastLogIndex() < context.getLogStore().getEndIndex() &&
            request.getLastLogTerm() == getTermForLogIndex(context.getLogStore(), request.getLastLogIndex()));

        boolean stateTransfer = false;
        if (request.getFlags().contains(MessageFlags.STATE_TRANSFER_REQUIRED) &&
                (request.getFlags().contains(MessageFlags.TRANSFER_SNAPSHOT) ||
                (request.getFlags().contains(MessageFlags.TRANSFER_LOG) && logOk))) {
            Assert.isTrue(request.getLogEntries() != null && request.getLogEntries().size() == 1);
            LogEntry logEntry = request.getLogEntries().get(0);
            StateTransferConfiguration stateTransferConfiguration = Utils.readStateTransferConfiguration(logEntry.getValue());

            long startLogIndex = 1;
            if (request.getFlags().contains(MessageFlags.TRANSFER_LOG))
                startLogIndex = request.getLastLogIndex() + 1;

            if (!stateTransferInProgress) {
                stateTransferInProgress = true;
                stateTransferProtocol.requestStateTransfer(stateTransferConfiguration.getHost(), stateTransferConfiguration.getPort(),
                    startLogIndex, new CompletionHandler() {
                        @Override
                        public void onSucceeded(Object result) {
                            stateTransferInProgress = false;
                        }

                        @Override
                        public void onFailed(Throwable error) {
                            stateTransferInProgress = false;
                        }
                    });
            }

            response.setFlags(Enums.of(MessageFlags.STATE_TRANSFER_REQUIRED));
            stateTransfer = true;
        }

        if (request.getTerm() < state.getServerState().getTerm() || !logOk || stateTransfer || stateTransferInProgress) {
            response.setAccepted(false);
            response.setNextIndex(context.getLogStore().getEndIndex());
            return response.toMessage();
        }

        if (request.getLogEntries() != null && !request.getLogEntries().isEmpty())
            appendEntries(request.getLogEntries(), request.getLastLogIndex() + 1);

        state.setLeader(request.getSource());

        commit(request.getCommitIndex());

        response.setAccepted(true);
        response.setNextIndex(request.getLastLogIndex() + (request.getLogEntries() == null ? 0 : request.getLogEntries().size()) + 1);
        return response.toMessage();
    }

    @Override
    public void handleAppendEntriesResponse(AppendEntriesResponse response) {
        if (state.getRole() != ServerRole.LEADER)
            return;

        IServerPeer peer = state.getPeers().get(response.getSource());
        if (peer == null)
            return;

        long rewindStep = peer.getRewindStep();
        peer.setRewindStep(1);

        boolean needToCatchup;
        if (response.isAccepted()) {
            peer.setNextLogIndex(response.getNextIndex());
            peer.setMatchedIndex(response.getNextIndex() - 1);

            ArrayList<Long> matchedIndexes = new ArrayList<>(state.getPeers().size() + 1);
            matchedIndexes.add(context.getLogStore().getEndIndex() - 1);
            for (IServerPeer serverPeer : state.getPeers().values())
                matchedIndexes.add(serverPeer.getMatchedIndex());

            matchedIndexes.sort(Comparator.reverseOrder());
            commit(matchedIndexes.get(matchedIndexes.size() / 2));

            needToCatchup = response.getNextIndex() < context.getLogStore().getEndIndex();
        } else {
            if (response.getFlags().contains(MessageFlags.STATE_TRANSFER_REQUIRED)) {
                peer.setNextLogIndex(response.getNextIndex());
                needToCatchup = false;
            } else {
                if (response.getNextIndex() < peer.getNextLogIndex())
                    peer.setNextLogIndex(response.getNextIndex());
                else if (peer.getNextLogIndex() > rewindStep) {
                    peer.setNextLogIndex(peer.getNextLogIndex() - rewindStep);
                    peer.setRewindStep(rewindStep * 2);
                }
                else
                    peer.setNextLogIndex(1);

                needToCatchup = true;
            }
        }

        if (needToCatchup)
            requestAppendEntries(peer);
    }

    @Override
    public void onSessionRemoved(ClientSession session) {
        if (logger.isLogEnabled(LogLevel.DEBUG))
            logger.log(LogLevel.DEBUG, marker, messages.sessionRemoved(session.getClientId()));

        queryProtocol.onSessionRemoved(session);
    }

    private void requestAppendEntries(IServerPeer peer) {
        if (peer.isConnected() && !peer.isRequestInProgress() )
            server.send(peer, createAppendEntriesRequest(peer));
    }

    private void appendEntries(List<LogEntry> logEntries, long startLogIndex) {
        long logIndex = startLogIndex;
        int index = 0;
        while (logIndex < context.getLogStore().getEndIndex() && index < logEntries.size() &&
                logEntries.get(index).getTerm() == context.getLogStore().getAt(logIndex).getTerm()) {
            index++;
            logIndex++;
        }

        List<LogEntryMapping> mappings = null;
        while (logIndex < context.getLogStore().getEndIndex() && index < logEntries.size()) {
            LogEntryMapping mapping = new LogEntryMapping();
            mapping.newEntry = logEntries.get(index);
            mapping.logIndex = logIndex;

            if (mappings == null)
                mappings = new ArrayList<>();
            mappings.add(mapping);

            logIndex++;
            index++;
        }

        if (mappings != null) {
            for (LogEntryMapping mapping : mappings) {
                LogEntry newEntry = mapping.newEntry;
                context.getLogStore().setAt(mapping.logIndex, newEntry);
            }
        }

        while (index < logEntries.size()) {
            LogEntry logEntry = logEntries.get(index);
            context.getLogStore().append(logEntry);

            index++;
        }
    }

    private AppendEntriesRequest createAppendEntriesRequest(IServerPeer peer) {
        Assert.checkState(state.getRole() == ServerRole.LEADER);

        long startLogIndex = context.getLogStore().getStartIndex();
        long currentNextIndex = context.getLogStore().getEndIndex();
        long commitIndex = state.getQuickCommitIndex();
        long term = state.getServerState().getTerm();

        long lastLogIndex = peer.getNextLogIndex() - 1;

        Assert.checkState(lastLogIndex < currentNextIndex);

        AppendEntriesRequestBuilder request = new AppendEntriesRequestBuilder();

        boolean logEntriesUnavailable = startLogIndex > 1 && lastLogIndex < startLogIndex;
        List<LogEntry> logEntries = null;
        long lastLogTerm = getTermForLogIndex(context.getLogStore(), lastLogIndex);
        if (logEntriesUnavailable ||
            (currentNextIndex - 1 - lastLogIndex >= context.getServerChannelFactoryConfiguration().getMinStateTransferGapLogEntryCount())) {

            if (logEntriesUnavailable)
                request.setFlags(Enums.of(MessageFlags.STATE_TRANSFER_REQUIRED, MessageFlags.TRANSFER_SNAPSHOT));
            else
                request.setFlags(Enums.of(MessageFlags.STATE_TRANSFER_REQUIRED, MessageFlags.TRANSFER_LOG));

            String host = stateTransferProtocol.getHost();
            int port = stateTransferProtocol.getPort();

            StateTransferConfiguration stateTransferConfiguration = new StateTransferConfiguration(host, port, 1);
            logEntries = Collections.singletonList(new LogEntry(0, Utils.writeStateTransferConfiguration(stateTransferConfiguration),
                LogValueType.APPLICATION, null, 0));
        } else {
            Assert.checkState(lastLogTerm >= 0);

            long endLogIndex = Math.min(currentNextIndex, lastLogIndex + 1 + context.getServerChannelFactoryConfiguration().getMaxPublishingLogEntryCount());

            if (lastLogIndex + 1 < endLogIndex) {
                logEntries = context.getLogStore().get(lastLogIndex + 1, endLogIndex);
            }
        }

        request.setGroupId(state.getConfiguration().getGroupId());
        request.setSource(context.getServerId());
        request.setLastLogIndex(lastLogIndex);
        request.setLastLogTerm(lastLogTerm);
        request.setLogEntries(logEntries);
        request.setCommitIndex(commitIndex);
        request.setTerm(term);
        return request.toMessage();
    }

    private void addInitialConfiguration() {
        GroupConfiguration configuration = state.getConfiguration();

        context.getLogStore().append(new LogEntry(state.getServerState().getTerm(), Utils.writeGroupConfiguration(configuration),
            LogValueType.CONFIGURATION, null, 0));

        state.setConfigurationChanging(true);

        if (logger.isLogEnabled(LogLevel.DEBUG))
            logger.log(LogLevel.DEBUG, marker, messages.addInitialConfiguration(configuration));
    }

    private void commit(long commitIndex) {
        if (commitIndex > state.getQuickCommitIndex()) {
            state.setQuickCommitIndex(commitIndex);

            if (state.getRole() == ServerRole.LEADER) {
                for (IServerPeer peer : state.getPeers().values())
                    requestAppendEntries(peer);
            }
        }

        localCommit();
    }

    private void localCommit() {
        if (commitsLocked)
            return;

        long currentCommitIndex = state.getServerState().getCommitIndex();
        if (currentCommitIndex >= state.getQuickCommitIndex() || currentCommitIndex >= context.getLogStore().getEndIndex() - 1)
            return;

        IStateMachineTransaction transaction = context.getStateMachine().beginTransaction(false);

        while (currentCommitIndex < state.getQuickCommitIndex() && currentCommitIndex < context.getLogStore().getEndIndex() - 1) {
            currentCommitIndex++;

            LogEntry logEntry = context.getLogStore().getAt(currentCommitIndex);
            if (logEntry.getValueType() == LogValueType.APPLICATION) {
                if (logEntry.getClientId() != null) {
                    ClientSession session = clientSessionManager.ensureSession(logEntry.getClientId());
                    session.commit(logEntry.getMessageId());
                }

                transaction.publish(currentCommitIndex, logEntry.getValue());
            } else if (logEntry.getValueType() == LogValueType.CONFIGURATION) {
                GroupConfiguration newConfiguration = Utils.readGroupConfiguration(logEntry.getValue());

                if (logger.isLogEnabled(LogLevel.DEBUG))
                    logger.log(LogLevel.DEBUG, marker, messages.committedConfiguration(newConfiguration, currentCommitIndex));

                membershipProtocol.commitConfiguration(newConfiguration);
                transaction.writeConfiguration(newConfiguration);
            }

            state.getServerState().setCommitIndex(currentCommitIndex);
        }

        transaction.writeState(state.getServerState());
        transaction.commit();

        context.getLogStore().commit(currentCommitIndex);

        for (ClientSession session : clientSessionManager.getSessions().reverseValues()) {
            if (!session.isCommitted())
                break;

            queryProtocol.onCommit(session);
            session.setCommitted(false);
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

    private static class LogEntryMapping {
        LogEntry newEntry;
        long logIndex;
    }

    private interface IMessages {
        @DefaultMessage("Initial configuration is added to log store: {0}")
        ILocalizedMessage addInitialConfiguration(GroupConfiguration configuration);

        @DefaultMessage("Configuration is committed at log index {1}: {0}")
        ILocalizedMessage committedConfiguration(GroupConfiguration configuration, long logIndex);

        @DefaultMessage("Detected an uncommitted configuration change at log index {0}.")
        ILocalizedMessage uncommittedConfigurationChange(long logIndex);

        @DefaultMessage("Client session has been removed: {0}")
        ILocalizedMessage sessionRemoved(UUID clientId);
    }
}
