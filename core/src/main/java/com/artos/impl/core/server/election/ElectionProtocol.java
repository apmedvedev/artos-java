package com.artos.impl.core.server.election;

import com.artos.api.core.server.conf.ServerChannelFactoryConfiguration;
import com.artos.impl.core.message.GroupTransientState;
import com.artos.impl.core.message.GroupTransientStateBuilder;
import com.artos.impl.core.message.MessageFlags;
import com.artos.impl.core.message.VoteRequest;
import com.artos.impl.core.message.VoteRequestBuilder;
import com.artos.impl.core.message.VoteResponse;
import com.artos.impl.core.message.VoteResponseBuilder;
import com.artos.impl.core.server.impl.Context;
import com.artos.impl.core.server.impl.IServer;
import com.artos.impl.core.server.impl.IServerPeer;
import com.artos.impl.core.server.impl.ServerRole;
import com.artos.impl.core.server.impl.State;
import com.artos.impl.core.server.membership.IMembershipProtocol;
import com.artos.impl.core.server.replication.IReplicationProtocol;
import com.artos.impl.core.server.utils.Utils;
import com.artos.spi.core.IStateMachineTransaction;
import com.exametrika.common.compartment.ICompartmentTimer;
import com.exametrika.common.compartment.impl.CompartmentTimer;
import com.exametrika.common.l10n.DefaultMessage;
import com.exametrika.common.l10n.ILocalizedMessage;
import com.exametrika.common.l10n.Messages;
import com.exametrika.common.log.ILogger;
import com.exametrika.common.log.IMarker;
import com.exametrika.common.log.LogLevel;
import com.exametrika.common.log.Loggers;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.Enums;
import com.exametrika.common.utils.Times;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

public class ElectionProtocol implements IElectionProtocol {
    private static final IMessages messages = Messages.get(IMessages.class);
    private final ILogger logger = Loggers.get(getClass());
    private final IMarker marker;
    private final IServer server;
    private final Context context;
    private final State state;
    private final Random random = new Random();
    private IMembershipProtocol membershipProtocol;
    private IReplicationProtocol replicationProtocol;
    private final Set<UUID> votedServers = new HashSet<>();
    private final Map<UUID, GroupTransientState> transientStates = new HashMap<>();
    private int votesGranted;
    private boolean electionCompleted;
    private final ICompartmentTimer electionTimer = new CompartmentTimer((currentTime) -> requestElection());
    private long nextVotingTime;
    private boolean electionLocked;
    private boolean preVotePhase;

    public ElectionProtocol(IServer server, Context context, State state) {
        Assert.notNull(server);
        Assert.notNull(context);
        Assert.notNull(state);

        this.server = server;
        this.context = context;
        this.state = state;
        this.marker = context.getMarker();
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

    @Override
    public void start() {
        restartElectionTimer();
    }

    @Override
    public void stop() {
        electionTimer.stop();
    }

    @Override
    public void onTimer(long currentTime) {
        electionTimer.onTimer(currentTime);
    }

    @Override
    public VoteResponse handleVoteRequest(VoteRequest request) {
        VoteResponseBuilder response = new VoteResponseBuilder();
        response.setSource(context.getServerId());
        response.setGroupId(state.getConfiguration().getGroupId());
        response.setTerm(state.getServerState().getTerm());
        response.setAccepted(false);

        boolean preVote = request.getFlags().contains(MessageFlags.PRE_VOTE);

        long currentTime = Times.getCurrentTime();
        if (state.getConfiguration().findServer(request.getSource()) == null || currentTime < nextVotingTime)
            return response.toMessage();

        if (!preVote)
            Utils.updateTerm(context, this, state, request.getTerm());

        response.setTerm(state.getServerState().getTerm());

        boolean requestLogNewerOrEqual = request.getLastLogTerm() > context.getLogStore().getLast().getTerm() ||
            (request.getLastLogTerm() == context.getLogStore().getLast().getTerm() &&
                request.getLastLogIndex() >= context.getLogStore().getEndIndex() - 1);

        boolean granted = request.getTerm() == state.getServerState().getTerm() && requestLogNewerOrEqual &&
            (preVote || (request.getSource().equals(state.getServerState().getVotedFor()) || state.getServerState().getVotedFor() == null));

        if (granted) {
            if (!preVote) {
                state.getServerState().setVotedFor(request.getSource());

                IStateMachineTransaction transaction = context.getStateMachine().beginTransaction(false);
                transaction.writeState(state.getServerState());
                transaction.commit();

                GroupTransientStateBuilder transientStateBuilder = new GroupTransientStateBuilder();
                replicationProtocol.acquireTransientState(transientStateBuilder);
                response.setTransientState(transientStateBuilder.toState());
            }

            response.setAccepted(true);
        }

        if (logger.isLogEnabled(LogLevel.DEBUG))
            logger.log(LogLevel.DEBUG, marker, messages.voteRequestReceived(
                Utils.toString(request, state, true), Utils.toString(response.toMessage(), state, true),
                context.getLogStore().getLast().toString(), context.getLogStore().getEndIndex(), state.getServerState().getTerm(), granted));

        return response.toMessage();
    }

    @Override
    public void handleVoteResponse(VoteResponse response) {
        if (electionCompleted)
            return;

        if (votedServers.contains(response.getSource()))
            return;

        votedServers.add(response.getSource());

        if (response.isAccepted()) {
            votesGranted += 1;

            if (!preVotePhase)
                transientStates.put(response.getSource(), response.getTransientState());
        }


        if (logger.isLogEnabled(LogLevel.DEBUG))
            logger.log(LogLevel.DEBUG, marker, messages.voteResponseReceived(Utils.toString(response, state, false),
                response.isAccepted(), votesGranted, votedServers.size()));

        if (votedServers.size() >= state.getPeers().size() + 1)
            electionCompleted();

        if (votesGranted > (state.getPeers().size() + 1) / 2) {
            if (preVotePhase)
                becomeCandidate();
            else
                becomeLeader();
        }
    }

    @Override
    public void delayElection() {
        restartElectionTimer();
        electionLocked = false;
        nextVotingTime = Times.getCurrentTime() + context.getServerChannelFactoryConfiguration().getMinElectionTimeout();
    }

    @Override
    public void becomeFollower() {
        clearElectionState();

        state.setRole(ServerRole.FOLLOWER);

        restartElectionTimer();

        membershipProtocol.onFollower();

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.serverSetAsFollower(state.getServerState().getTerm()));
    }

    @Override
    public void lockElection() {
        electionLocked = true;
    }

    private void restartElectionTimer() {
        if (state.isCatchingUp())
            return;

        ServerChannelFactoryConfiguration serverChannelFactoryConfiguration = context.getServerChannelFactoryConfiguration();
        int electionTimeout = serverChannelFactoryConfiguration.getMinElectionTimeout() +
            random.nextInt(serverChannelFactoryConfiguration.getMaxElectionTimeout() - serverChannelFactoryConfiguration.getMinElectionTimeout() + 1);

        electionTimer.setPeriod(electionTimeout);
        electionTimer.restart();
    }

    private void requestElection() {
        Assert.checkState(state.getRole() != ServerRole.LEADER);

        if (state.isCatchingUp())
            return;

        if (electionLocked)
            return;

        requestPreVoteForSelf();
    }

    private void becomeLeader() {
        electionCompleted();

        electionTimer.stop();

        state.setRole(ServerRole.LEADER);
        state.setLeader(context.getServerId());

        replicationProtocol.applyTransientState(transientStates.values());
        transientStates.clear();

        membershipProtocol.onLeader();

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.serverElectedAsLeader(state.getServerState().getTerm()));
    }

    private void becomeCandidate() {
        if (state.getRole() != ServerRole.CANDIDATE) {
            state.getServerState().increaseTerm();
            state.getServerState().setVotedFor(null);
            state.setRole(ServerRole.CANDIDATE);

            IStateMachineTransaction transaction = context.getStateMachine().beginTransaction(false);
            transaction.writeState(state.getServerState());
            transaction.commit();

            if (logger.isLogEnabled(LogLevel.DEBUG))
                logger.log(LogLevel.DEBUG, marker, messages.serverSetAsCandidate(state.getServerState().getTerm()));

            requestVoteForSelf();
        }
    }

    private void requestPreVoteForSelf() {
        restartElectionTimer();
        clearElectionState();

        if (logger.isLogEnabled(LogLevel.DEBUG))
            logger.log(LogLevel.DEBUG, marker, messages.requestPreVoteStarted(state.getServerState().getTerm()));

        if (state.getPeers().isEmpty()) {
            becomeCandidate();
            return;
        }

        preVotePhase = true;

        requestVotes(true);
    }

    private void requestVoteForSelf() {
        restartElectionTimer();
        clearElectionState();

        if (logger.isLogEnabled(LogLevel.DEBUG))
            logger.log(LogLevel.DEBUG, marker, messages.requestVoteStarted(state.getServerState().getTerm()));

        if (state.getPeers().isEmpty()) {
            becomeLeader();
            return;
        }

        state.getServerState().setVotedFor(context.getServerId());

        IStateMachineTransaction transaction = context.getStateMachine().beginTransaction(false);
        transaction.writeState(state.getServerState());
        transaction.commit();

        requestVotes(false);
    }

    private void requestVotes(boolean preVote) {
        votesGranted = 1;
        votedServers.add(context.getServerId());

        for (IServerPeer peer : state.getPeers().values()) {
            VoteRequestBuilder request = new VoteRequestBuilder();
            request.setSource(context.getServerId());
            request.setGroupId(state.getConfiguration().getGroupId());
            request.setLastLogIndex(context.getLogStore().getEndIndex() - 1);
            request.setLastLogTerm(context.getLogStore().getLast().getTerm());
            request.setTerm(state.getServerState().getTerm());

            if (preVote)
                request.setFlags(Enums.of(MessageFlags.PRE_VOTE));

            server.send(peer, request.toMessage());
        }
    }

    private void clearElectionState() {
        electionCompleted = false;
        votesGranted = 0;
        votedServers.clear();
        transientStates.clear();
        preVotePhase = false;
    }

    private void electionCompleted() {
        preVotePhase = false;

        if (!electionCompleted) {
            electionCompleted = true;

            if (logger.isLogEnabled(LogLevel.DEBUG))
                logger.log(LogLevel.DEBUG, marker, messages.electionCompleted(state.getServerState().getTerm()));
        }
    }

    private interface IMessages {
        @DefaultMessage("Request vote for self is started for term {0}.")
        ILocalizedMessage requestVoteStarted(long term);

        @DefaultMessage("Request pre-vote for self is started for term {0}.")
        ILocalizedMessage requestPreVoteStarted(long term);

        @DefaultMessage("Server is elected as a leader for term {0}.")
        ILocalizedMessage serverElectedAsLeader(long term);

        @DefaultMessage("Server is set as a follower for term {0}.")
        ILocalizedMessage serverSetAsFollower(long term);

        @DefaultMessage("Election has been completed for term {0}.")
        ILocalizedMessage electionCompleted(long term);

        @DefaultMessage("Election has been started. Server is set as candidate for term {0}.")
        ILocalizedMessage serverSetAsCandidate(long term);

        @DefaultMessage("Vote response has been received: {0}. Accepted: {1}, votes granted: {2}, voted servers: {3}.")
        ILocalizedMessage voteResponseReceived(String response, boolean accepted, int votesGranted, int votedServers);

        @DefaultMessage("Vote request has been received: {0}\n    Response: {1}\n    Last entry: {2}\n    End-log-index: {3}, term: {4}, granted: {5}")
        ILocalizedMessage voteRequestReceived(String request, String response, String lastEntry, long endLogIndex, long term, boolean granted);
    }
}
