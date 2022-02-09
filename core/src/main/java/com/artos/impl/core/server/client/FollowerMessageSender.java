/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.server.client;

import com.artos.api.core.server.conf.ServerConfiguration;
import com.artos.impl.core.message.FollowerRequest;
import com.artos.impl.core.message.FollowerResponse;
import com.artos.impl.core.message.Message;
import com.artos.impl.core.server.impl.Context;
import com.artos.impl.core.server.impl.State;
import com.artos.impl.core.server.utils.MessageUtils;
import com.artos.impl.core.server.utils.Utils;
import com.artos.spi.core.IMessageSender;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class FollowerMessageSender implements IFollowerMessageSender {
    private static final IMessages messages = Messages.get(IMessages.class);
    private final ILogger logger = Loggers.get(getClass());
    private final IMarker marker;
    private final Context context;
    private final State state;
    private final Random random = new Random();
    private final ICompletionHandler<FollowerResponse> responseHandler;
    private UUID leaderId;
    private String leaderEndpoint;
    private ServerConfiguration leaderConfiguration;
    private IMessageSender messageSender;

    public FollowerMessageSender(Context context, State state, ICompletionHandler<FollowerResponse> responseHandler) {
        Assert.notNull(context);
        Assert.notNull(state);
        Assert.notNull(responseHandler);

        this.context = context;
        this.state = state;
        this.responseHandler = responseHandler;
        this.marker = Loggers.getMarker(context.getLocalServer().getEndpoint());
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
        onLeaderDisconnected();
    }

    @Override
    public void send(FollowerRequest request) {
        IMessageSender messageSender = ensureMessageSender();
        if (messageSender == null)
            return;

        ServerConfiguration leaderConfiguration = this.leaderConfiguration;

        if (logger.isLogEnabled(LogLevel.TRACE))
            logger.log(LogLevel.TRACE, marker, messages.requestSent(Utils.toString(request, state, true), leaderConfiguration.getEndpoint()));

        ByteArray serializedRequest = MessageUtils.write(request);

        messageSender.send(serializedRequest);
    }

    private void handleSucceededResponse(FollowerResponse response, ICompletionHandler<FollowerResponse> completionHandler) {
        if (!response.getSource().equals(leaderId)) {
            completionHandler.onFailed(new SystemException(messages.responseOutdated()));
            return;
        }

        if (logger.isLogEnabled(LogLevel.TRACE))
            logger.log(LogLevel.TRACE, marker, messages.responseReceived(Utils.toString(response, state, true)));

        if (response.isAccepted()) {
            completionHandler.onSucceeded(response);
        } else {
            if (response.getLeaderId() == null || !response.getLeaderId().equals(leaderId)) {
                if (logger.isLogEnabled(LogLevel.DEBUG))
                    logger.log(LogLevel.DEBUG, marker, messages.endpointNotLeader(leaderEndpoint, leaderId));

                onLeaderDisconnected();

                if (response.getLeaderId() != null)
                    leaderId = response.getLeaderId();
            }

            completionHandler.onFailed(new SystemException(messages.requestNotAccepted()));
        }
    }

    private void handleFailedResponse(String endpoint, Throwable error, ICompletionHandler<FollowerResponse> completionHandler) {
        if (logger.isLogEnabled(LogLevel.WARNING))
            logger.log(LogLevel.WARNING, marker, messages.responseErrorReceived(endpoint), error);

        onLeaderDisconnected();

        completionHandler.onFailed(error);
    }

    private void onLeaderDisconnected() {
        if (leaderId != null) {
            if (logger.isLogEnabled(LogLevel.INFO))
                logger.log(LogLevel.INFO, marker, messages.leaderDisconnected(leaderEndpoint, leaderId));

            leaderId = null;
            leaderConfiguration = null;
            stopMessageSender();
        }
    }

    private IMessageSender ensureMessageSender() {
        if (messageSender == null) {
            ServerConfiguration configuration = ensureLeaderConfiguration();
            try {
                messageSender = context.getMessageSenderFactory().createSender(configuration.getEndpoint(), new CompletionHandler<ByteArray>() {
                    @Override
                    public boolean isCanceled() {
                        return responseHandler.isCanceled();
                    }

                    @Override
                    public void onSucceeded(ByteArray response) {
                        Message deserializedResponse = MessageUtils.read(response);

                        context.getCompartment().offer(() -> handleSucceededResponse((FollowerResponse)deserializedResponse,
                            responseHandler));
                    }

                    @Override
                    public void onFailed(Throwable error) {
                        context.getCompartment().offer(() -> handleFailedResponse(configuration.getEndpoint(), error,
                            responseHandler));
                    }
                });
                messageSender.start();

                leaderEndpoint = configuration.getEndpoint();

                if (logger.isLogEnabled(LogLevel.INFO))
                    logger.log(LogLevel.INFO, marker, messages.leaderConnected(leaderEndpoint, leaderId));
            } catch (Exception e) {
                responseHandler.onFailed(e);

                onLeaderDisconnected();
                return null;
            }
        }

        return messageSender;
    }

    private void stopMessageSender() {
        if (messageSender != null) {
            messageSender.stop();
            messageSender = null;
        }
    }

    private ServerConfiguration ensureLeaderConfiguration() {
        if (leaderId == null)
            leaderId = state.getLeader();

        ServerConfiguration configuration = selectLeaderConfiguration(leaderId);
        leaderId = configuration.getId();
        leaderConfiguration = configuration;
        return configuration;
    }

    private ServerConfiguration selectLeaderConfiguration(UUID leaderId) {
        Assert.checkState(state.getConfiguration().getServers().size() > 1);

        List<ServerConfiguration> servers = new ArrayList<>(state.getConfiguration().getServers());
        servers.remove(context.getLocalServer());

        if (leaderId != null) {
            for (ServerConfiguration serverConfiguration : servers) {
                if (serverConfiguration.getId().equals(leaderId))
                    return serverConfiguration;
            }
        }

        int index = random.nextInt(servers.size());
        ServerConfiguration serverConfiguration = servers.get(index);

        if (logger.isLogEnabled(LogLevel.DEBUG))
            logger.log(LogLevel.DEBUG, marker, messages.randomServerSelected(serverConfiguration));

        return serverConfiguration;
    }

    private interface IMessages {
        @DefaultMessage("Request {0} sent to {1}.")
        ILocalizedMessage requestSent(String request, String destination);

        @DefaultMessage("Response received: {0}.")
        ILocalizedMessage responseReceived(String response);

        @DefaultMessage("Response error received from server ''{0}''.")
        ILocalizedMessage responseErrorReceived(String serverId);

        @DefaultMessage("Random server has been selected as primary group endpoint: {0}.")
        ILocalizedMessage randomServerSelected(ServerConfiguration serverConfiguration);

        @DefaultMessage("Request is not accepted.")
        ILocalizedMessage requestNotAccepted();

        @DefaultMessage("Response is outdated.")
        ILocalizedMessage responseOutdated();

        @DefaultMessage("Group endpoint has been connected: {0}:{1}.")
        ILocalizedMessage leaderConnected(String leaderEndpoint, UUID leaderId);

        @DefaultMessage("Group endpoint has been disconnected: {0}:{1}.")
        ILocalizedMessage leaderDisconnected(String leaderEndpoint, UUID leaderId);

        @DefaultMessage("Group endpoint is not a leader: {0}:{1}.")
        ILocalizedMessage endpointNotLeader(String leaderEndpoint, UUID leaderId);
    }
}
