/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.server.impl;

import com.artos.api.core.server.conf.ServerConfiguration;
import com.artos.impl.core.message.AppendEntriesRequest;
import com.artos.impl.core.message.AppendEntriesResponse;
import com.artos.impl.core.message.JoinGroupRequest;
import com.artos.impl.core.message.LeaveGroupRequest;
import com.artos.impl.core.message.Message;
import com.artos.impl.core.message.PublishRequest;
import com.artos.impl.core.message.QueryRequest;
import com.artos.impl.core.message.QueryResponse;
import com.artos.impl.core.message.Request;
import com.artos.impl.core.message.Response;
import com.artos.impl.core.message.ServerRequest;
import com.artos.impl.core.message.ServerResponse;
import com.artos.impl.core.message.SubscriptionRequest;
import com.artos.impl.core.message.SubscriptionResponse;
import com.artos.impl.core.message.VoteRequest;
import com.artos.impl.core.message.VoteResponse;
import com.artos.impl.core.server.client.ILeaderClientProtocol;
import com.artos.impl.core.server.election.IElectionProtocol;
import com.artos.impl.core.server.membership.IJoinGroupProtocol;
import com.artos.impl.core.server.membership.ILeaveGroupProtocol;
import com.artos.impl.core.server.replication.IQueryProtocol;
import com.artos.impl.core.server.replication.IReplicationProtocol;
import com.artos.impl.core.server.state.IStateTransferProtocol;
import com.artos.impl.core.server.utils.MessageUtils;
import com.artos.impl.core.server.utils.Utils;
import com.artos.spi.core.IMessageListener;
import com.artos.spi.core.IMessageReceiver;
import com.exametrika.common.compartment.ICompartmentTimerProcessor;
import com.exametrika.common.l10n.DefaultMessage;
import com.exametrika.common.l10n.ILocalizedMessage;
import com.exametrika.common.l10n.Messages;
import com.exametrika.common.l10n.SystemException;
import com.exametrika.common.log.ILogger;
import com.exametrika.common.log.IMarker;
import com.exametrika.common.log.LogLevel;
import com.exametrika.common.log.Loggers;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.CompletionHandler;
import com.exametrika.common.utils.ICompletionHandler;
import com.exametrika.common.utils.ILifecycle;

public class Server implements IServer, IMessageReceiver, ICompartmentTimerProcessor, ILifecycle {
    private static final IMessages messages = Messages.get(IMessages.class);
    private final ILogger logger = Loggers.get(getClass());
    private final Context context;
    private final State state;
    private final IMarker marker;
    private IElectionProtocol electionProtocol;
    private IReplicationProtocol replicationProtocol;
    private IStateTransferProtocol stateTransferProtocol;
    private IJoinGroupProtocol joinGroupProtocol;
    private ILeaveGroupProtocol leaveGroupProtocol;
    private ILeaderClientProtocol leaderClientProtocol;
    private IQueryProtocol queryProtocol;
    private IMessageListener messageListener;
    private boolean started;

    public Server(Context context, State state) {
        Assert.notNull(context);
        Assert.notNull(state);

        this.context = context;
        this.state = state;
        this.marker = context.getMarker();
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

    public void setStateTransferProtocol(IStateTransferProtocol stateTransferProtocol) {
        Assert.notNull(stateTransferProtocol);
        Assert.isNull(this.stateTransferProtocol);

        this.stateTransferProtocol = stateTransferProtocol;
    }

    public void setJoinGroupProtocol(IJoinGroupProtocol joinGroupProtocol) {
        Assert.notNull(joinGroupProtocol);
        Assert.isNull(this.joinGroupProtocol);

        this.joinGroupProtocol = joinGroupProtocol;
    }

    public void setLeaveGroupProtocol(ILeaveGroupProtocol leaveGroupProtocol) {
        Assert.notNull(leaveGroupProtocol);
        Assert.isNull(this.leaveGroupProtocol);

        this.leaveGroupProtocol = leaveGroupProtocol;
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

    @Override
    public void start() {
        context.getLogStore().start();

        replicationProtocol.start();
        electionProtocol.start();
        joinGroupProtocol.start();
        leaveGroupProtocol.start();
        stateTransferProtocol.start();
        queryProtocol.start();

        context.getStateMachine().start();

        messageListener = context.getMessageListenerFactory().createListener(this);
        messageListener.start();

        started = true;
    }

    @Override
    public void stop() {
        started = false;

        messageListener.stop();

        for (IServerPeer peer : state.getPeers().values())
            peer.stop();

        context.getStateMachine().stop();

        replicationProtocol.stop();
        electionProtocol.stop();
        joinGroupProtocol.stop();
        leaveGroupProtocol.stop();
        stateTransferProtocol.stop();
        queryProtocol.stop();

        context.getLogStore().stop();
    }

    @Override
    public void onTimer(long currentTime) {
        if (!started)
            return;

        electionProtocol.onTimer(currentTime);
        replicationProtocol.onTimer(currentTime);
        joinGroupProtocol.onTimer(currentTime);
        leaveGroupProtocol.onTimer(currentTime);
        leaderClientProtocol.onTimer(currentTime);
        queryProtocol.onTimer(currentTime);
    }

    @Override
    public void receive(ByteArray request, ICompletionHandler<ByteArray> response) {
        if (!started)
            return;

        Message deserializedRequest = MessageUtils.read(request);

        context.getCompartment().offer(() -> {
            if (isAsyncRequest(deserializedRequest))
                handleAsync((Request) deserializedRequest, response);
            else
                handleSync((Request) deserializedRequest, response);
        });
    }

    @Override
    public IServerPeer createPeer(ServerConfiguration configuration) {
        return new ServerPeer(configuration, context, new CompletionHandler<ServerResponse>() {
            @Override
            public void onSucceeded(ServerResponse response) {
                if (!response.getGroupId().equals(state.getConfiguration().getGroupId()))
                    return;

                if (logger.isLogEnabled(LogLevel.TRACE))
                    logger.log(LogLevel.TRACE, marker, messages.responseReceived(Utils.toString(response, state, true)));

                try {
                    handleResponse(response);
                } catch (Throwable e) {
                    if (logger.isLogEnabled(LogLevel.ERROR))
                        logger.log(LogLevel.ERROR, marker, e);
                }
            }

            @Override
            public void onFailed(Throwable error) {
                if (logger.isLogEnabled(LogLevel.WARNING))
                    logger.log(LogLevel.WARNING, marker, messages.responseErrorReceived(configuration.getEndpoint()), error);
            }
        });
    }

    @Override
    public void send(IServerPeer peer, ServerRequest request) {
        if (!started)
            return;

        if (logger.isLogEnabled(LogLevel.TRACE))
            logger.log(LogLevel.TRACE, marker, messages.requestSent(Utils.toString(request, state, true), peer.getConfiguration().getEndpoint()));

        peer.send(request);
    }

    private void handleAsync(Request request, ICompletionHandler<ByteArray> response) {
        if (!request.getGroupId().equals(state.getConfiguration().getGroupId())) {
            response.onFailed(new SystemException(messages.invalidGroupId()));
            return;
        }

        if (logger.isLogEnabled(LogLevel.TRACE))
            logger.log(LogLevel.TRACE, marker, messages.requestReceived(Utils.toString(request, state, true)));

        handleAsyncRequest(request, new CompletionHandler<Response>() {
            @Override
            public boolean isCanceled() {
                return response.isCanceled();
            }

            @Override
            public void onSucceeded(Response result) {
                ByteArray serializedResponse = MessageUtils.write(result);

                if (logger.isLogEnabled(LogLevel.TRACE))
                    logger.log(LogLevel.TRACE, marker, messages.responseSent(Utils.toString(result, state, true),
                            Utils.toString(request.getSource(), state)));

                response.onSucceeded(serializedResponse);
            }

            @Override
            public void onFailed(Throwable error) {
                if (logger.isLogEnabled(LogLevel.ERROR))
                    logger.log(LogLevel.ERROR, marker, error);

                response.onFailed(error);
            }
        });
    }

    private void handleSync(Request request, ICompletionHandler<ByteArray> response) {
        try {
            Assert.checkState(request.getGroupId().equals(state.getConfiguration().getGroupId()));

            if (logger.isLogEnabled(LogLevel.TRACE))
                logger.log(LogLevel.TRACE, marker, messages.requestReceived(Utils.toString(request, state, true)));

            Response result = handleSyncRequest(request);
            ByteArray serializedResponse = MessageUtils.write(result);

            if (logger.isLogEnabled(LogLevel.TRACE))
                logger.log(LogLevel.TRACE, marker, messages.responseSent(Utils.toString(result, state, true),
                    Utils.toString(request.getSource(), state)));

            response.onSucceeded(serializedResponse);
        } catch (Exception e) {
            if (logger.isLogEnabled(LogLevel.ERROR))
                logger.log(LogLevel.ERROR, marker, e);

            response.onFailed(e);
        }
    }

    private boolean isAsyncRequest(Message request) {
        if (request instanceof QueryRequest)
            return true;
        else if (request instanceof SubscriptionRequest)
            return true;
        else
            return false;
    }

    private void handleAsyncRequest(Request request, ICompletionHandler<? extends Response> response) {
        if (request instanceof QueryRequest)
            queryProtocol.handleQueryRequest((QueryRequest) request, (ICompletionHandler<QueryResponse>) response);
        else if (request instanceof SubscriptionRequest)
            queryProtocol.handleSubscriptionRequest((SubscriptionRequest) request, (ICompletionHandler<SubscriptionResponse>) response);
        else
            Assert.error();
    }

    private Response handleSyncRequest(Request request) {
        if (request instanceof AppendEntriesRequest)
            return replicationProtocol.handleAppendEntriesRequest((AppendEntriesRequest)request);
        else if (request instanceof VoteRequest)
            return electionProtocol.handleVoteRequest((VoteRequest)request);
        else if (request instanceof PublishRequest)
            return replicationProtocol.handlePublishRequest((PublishRequest)request);
        else if (request instanceof JoinGroupRequest)
            return joinGroupProtocol.handleJoinGroupRequest((JoinGroupRequest)request);
        else if (request instanceof LeaveGroupRequest)
            return leaveGroupProtocol.handleLeaveGroupRequest((LeaveGroupRequest)request);
        else
            return Assert.error();
    }

    private void handleResponse(ServerResponse response) {
        if (!checkTerm(response))
            return;

        if (response instanceof VoteResponse)
            electionProtocol.handleVoteResponse((VoteResponse)response);
        else if (response instanceof AppendEntriesResponse)
            replicationProtocol.handleAppendEntriesResponse((AppendEntriesResponse)response);
        else
            Assert.error();
    }

    private boolean checkTerm(ServerResponse response) {
        long term;
        if (response instanceof VoteResponse)
            term = ((VoteResponse) response).getTerm();
        else if (response instanceof AppendEntriesResponse)
            term = ((AppendEntriesResponse) response).getTerm();
        else
            return false;

        if (Utils.updateTerm(context, electionProtocol, state, term))
            return false;

        if (term < state.getServerState().getTerm()) {
            if (logger.isLogEnabled(LogLevel.DEBUG))
                logger.log(LogLevel.DEBUG, marker, messages.responseWithLowerTermReceived(Utils.toString(response, state, true),
                    state.getServerState().getTerm()));
            return false;
        }

        return true;
    }

    private interface IMessages {
        @DefaultMessage("Request received: {0}.")
        ILocalizedMessage requestReceived(String request);

        @DefaultMessage("Request {0} sent to {1}.")
        ILocalizedMessage requestSent(String request, String destination);

        @DefaultMessage("Response {0} sent to {1}.")
        ILocalizedMessage responseSent(String response, String destination);

        @DefaultMessage("Response received: {0}.")
        ILocalizedMessage responseReceived(String response);

        @DefaultMessage("Response error received from server ''{0}''.")
        ILocalizedMessage responseErrorReceived(String serverId);

        @DefaultMessage("Received response ''{0}'' has lower term than term of current server ''{1}''.")
        ILocalizedMessage responseWithLowerTermReceived(String response, long term);

        @DefaultMessage("Received group-id is not a group-id of local server.")
        ILocalizedMessage invalidGroupId();
    }
}
