/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */package com.artos.impl.core.server.impl;

import com.artos.api.core.server.conf.ServerConfiguration;
import com.artos.impl.core.message.Message;
import com.artos.impl.core.message.ServerRequest;
import com.artos.impl.core.message.ServerResponse;
import com.artos.impl.core.server.utils.MessageUtils;
import com.artos.spi.core.IMessageSender;
import com.exametrika.common.compartment.ICompartment;
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
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.CompletionHandler;
import com.exametrika.common.utils.ICompletionHandler;
import com.exametrika.common.utils.Times;

public class ServerPeer implements IServerPeer {
    private static final IMessages messages = Messages.get(IMessages.class);
    private final ILogger logger = Loggers.get(getClass());
    private final IMarker marker;
    private final ServerConfiguration configuration;
    private final ICompletionHandler<ServerResponse> responseHandler;
    private final ICompartment compartment;
    private IMessageSender messageSender;
    private final long heartbeatPeriod;
    private final int sendBackoffPeriod;
    private final int maxHeartbeatPeriod;
    private final Context context;
    private boolean heartbeatEnabled;
    private long currentHeartbeatPeriod;
    private long nextHeartbeatTime;
    private long nextLogIndex;
    private long matchedIndex;
    private long rewindStep = 1;
    private long lastResponseTime;
    private boolean inProgress;
    private final ICompartmentTimer peerTimeoutTimer = new CompartmentTimer((currentTime) -> handlePeerTimeout());

    public ServerPeer(ServerConfiguration configuration, Context context, ICompletionHandler<ServerResponse> responseHandler) {
        Assert.notNull(configuration);
        Assert.notNull(context);
        Assert.notNull(responseHandler);

        this.configuration = configuration;
        this.context = context;
        this.responseHandler = responseHandler;
        this.compartment = context.getCompartment();
        this.heartbeatPeriod = context.getServerChannelFactoryConfiguration().getHeartbeatPeriod();
        this.currentHeartbeatPeriod = heartbeatPeriod;
        this.maxHeartbeatPeriod = context.getServerChannelFactoryConfiguration().getMaxHeartbeatPeriod();
        this.sendBackoffPeriod = context.getServerChannelFactoryConfiguration().getSendFailureBackoff();
        this.nextLogIndex = 1;
        this.matchedIndex = 0;
        this.heartbeatEnabled = false;
        this.peerTimeoutTimer.setPeriod(context.getServerChannelFactoryConfiguration().getPeerTimeout());
        this.lastResponseTime = Times.getCurrentTime();
        this.marker = context.getMarker();
    }

    @Override
    public ServerConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public boolean isConnected() {
        return messageSender != null;
    }

    @Override
    public boolean isRequestInProgress() {
        return inProgress;
    }

    @Override
    public void start() {
        heartbeatEnabled = true;

        resumeHeartbeating();

        updateNextHeartbeatTime();

        inProgress = false;
        lastResponseTime = Times.getCurrentTime();
    }

    @Override
    public void stop() {
        heartbeatEnabled = false;

        onPeerDisconnected();
    }

    @Override
    public boolean canHeartbeat(long currentTime) {
        peerTimeoutTimer.onTimer(currentTime);

        if (!heartbeatEnabled)
            return false;

        if (currentTime >= nextHeartbeatTime) {
            IMessageSender messageSender = ensureMessageSender();
            if (messageSender != null)
                return true;
        }

        return false;
    }

    @Override
    public long getLastResponseTime() {
        return lastResponseTime;
    }

    @Override
    public long getNextLogIndex() {
        return nextLogIndex;
    }

    @Override
    public void setNextLogIndex(long nextLogIndex) {
        this.nextLogIndex = nextLogIndex;
    }

    @Override
    public long getRewindStep() {
        return rewindStep;
    }

    @Override
    public void setRewindStep(long value) {
        this.rewindStep = value;
    }

    @Override
    public long getMatchedIndex() {
        return matchedIndex;
    }

    @Override
    public void setMatchedIndex(long matchedIndex) {
        this.matchedIndex = matchedIndex;
    }

    @Override
    public void send(ServerRequest request) {
        updateNextHeartbeatTime();

        IMessageSender messageSender = ensureMessageSender();
        if (messageSender == null)
            return;

        ByteArray serializedRequest = MessageUtils.write(request);
        inProgress = true;

        messageSender.send(serializedRequest);
    }

    @Override
    public String toString() {
        return configuration.toString();
    }

    private void handleSucceededResponse(ServerResponse response, ICompletionHandler<ServerResponse> completionHandler) {
        inProgress = false;
        resumeHeartbeating();
        lastResponseTime = Times.getCurrentTime();
        peerTimeoutTimer.restart();
        completionHandler.onSucceeded(response);
    }

    private void handleFailedResponse(Throwable error, ICompletionHandler<ServerResponse> completionHandler) {
        inProgress = false;
        slowDownHeartbeating();
        lastResponseTime = Times.getCurrentTime();
        onPeerDisconnected();
        completionHandler.onFailed(error);
    }

    private void updateNextHeartbeatTime() {
        nextHeartbeatTime = Times.getCurrentTime() + currentHeartbeatPeriod;
    }

    private void slowDownHeartbeating() {
        this.currentHeartbeatPeriod = Math.min(maxHeartbeatPeriod, currentHeartbeatPeriod + sendBackoffPeriod);
        updateNextHeartbeatTime();
    }

    private void resumeHeartbeating() {
        if (currentHeartbeatPeriod > heartbeatPeriod) {
            currentHeartbeatPeriod = heartbeatPeriod;
            updateNextHeartbeatTime();
        }
    }

    private IMessageSender ensureMessageSender() {
        if (messageSender == null) {
            try {
                messageSender = context.getMessageSenderFactory().createSender(configuration.getEndpoint(), new CompletionHandler<ByteArray>() {
                    @Override
                    public boolean isCanceled() {
                        return responseHandler.isCanceled();
                    }

                    @Override
                    public void onSucceeded(ByteArray response) {
                        Message deserializedResponse = MessageUtils.read(response);

                        compartment.offer(() -> handleSucceededResponse((ServerResponse) deserializedResponse, responseHandler));
                    }

                    @Override
                    public void onFailed(Throwable error) {
                        compartment.offer(() -> handleFailedResponse(error, responseHandler));
                    }
                });
                onPeerConnected();
            } catch (Exception e) {
                onPeerDisconnected();

                responseHandler.onFailed(e);

                return null;
            }
        }

        return messageSender;
    }

    private void onPeerConnected() {
        messageSender.start();
        inProgress = false;
        peerTimeoutTimer.start();

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.peerConnected(configuration));
    }

    private void onPeerDisconnected() {
        if (messageSender == null)
            return;

        messageSender.stop();
        messageSender = null;
        inProgress = false;
        peerTimeoutTimer.stop();

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.peerDisconnected(configuration));
    }

    private void handlePeerTimeout() {
        if (logger.isLogEnabled(LogLevel.WARNING))
            logger.log(LogLevel.WARNING, marker, messages.peerTimeout(configuration.getEndpoint()));

        onPeerDisconnected();
    }

    private interface IMessages {
        @DefaultMessage("Timeout has occured for peer ''{0}''.")
        ILocalizedMessage peerTimeout(String endpoint);

        @DefaultMessage("Peer has been connected: ''{0}''.")
        ILocalizedMessage peerConnected(ServerConfiguration configuration);

        @DefaultMessage("Peer has been disconnected: ''{0}''.")
        ILocalizedMessage peerDisconnected(ServerConfiguration configuration);
    }
}
