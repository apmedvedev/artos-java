/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.client;

import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.api.core.server.conf.ServerConfiguration;
import com.artos.impl.core.message.ClientRequest;
import com.artos.impl.core.message.ClientResponse;
import com.artos.impl.core.message.Message;
import com.artos.impl.core.message.MessageFlags;
import com.artos.impl.core.message.PublishRequest;
import com.artos.impl.core.message.PublishRequestBuilder;
import com.artos.impl.core.message.PublishResponse;
import com.artos.impl.core.server.utils.MessageUtils;
import com.artos.spi.core.IClientStore;
import com.artos.spi.core.IMessageSender;
import com.artos.spi.core.IMessageSenderFactory;
import com.exametrika.common.compartment.ICompartment;
import com.exametrika.common.compartment.ICompartmentTimer;
import com.exametrika.common.compartment.ICompartmentTimerProcessor;
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
import com.exametrika.common.utils.Enums;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class ClientProtocol implements IClientProtocol, ICompartmentTimerProcessor {
    private static final IMessages messages = Messages.get(IMessages.class);
    private final ILogger logger = Loggers.get(getClass());
    private final IMarker marker;
    private final IMessageSenderFactory messageSenderFactory;
    private final UUID clientId;
    private final UUID groupId;
    private final IClientStore clientStore;
    private final Random random = new Random();
    private final GroupConfiguration initialConfiguration;
    private ICompartment compartment;
    private final ICompartmentTimer leaderTimeoutTimer = new CompartmentTimer((currentTime) -> handleLeaderTimeout());
    private final ICompartmentTimer heartbeatTimer = new CompartmentTimer((currentTime) -> requestHeartbeat());
    private final ICompartmentTimer updateConfigurationTimer = new CompartmentTimer((currentTime) -> requestConfigurationUpdate());
    private IPublishClientProtocol publishProtocol;
    private IQueryClientProtocol queryProtocol;
    private UUID leaderId;
    private String leaderEndpoint;
    private IMessageSender messageSender;
    private GroupConfiguration configuration;
    private List<ServerConfiguration> serverConfigurations = new ArrayList<>();
    private boolean inProgress;

    public ClientProtocol(IMessageSenderFactory messageSenderFactory, GroupConfiguration initialConfiguration,
                          String endpoint, UUID clientId, UUID groupId, long acknowledgeRequestPeriod, long updateConfigurationPeriod,
                          long leaderTimeout, IClientStore clientStore) {
        Assert.notNull(messageSenderFactory);
        Assert.notNull(initialConfiguration);
        Assert.notNull(endpoint);
        Assert.notNull(clientId);
        Assert.notNull(groupId);

        this.messageSenderFactory = messageSenderFactory;
        this.clientId = clientId;
        this.groupId = groupId;
        this.marker = Loggers.getMarker(endpoint);
        this.leaderTimeoutTimer.setPeriod(leaderTimeout);
        this.heartbeatTimer.setPeriod(acknowledgeRequestPeriod);
        this.clientStore = clientStore;
        this.initialConfiguration = initialConfiguration;
        this.configuration = initialConfiguration;
        this.updateConfigurationTimer.setPeriod(updateConfigurationPeriod);
    }

    public void setCompartment(ICompartment compartment) {
        Assert.notNull(compartment);
        Assert.isNull(this.compartment);

        this.compartment = compartment;
    }

    @Override
    public IPublishClientProtocol getPublishProtocol() {
        return publishProtocol;
    }

    public void setPublishProtocol(IPublishClientProtocol publishProtocol) {
        Assert.notNull(publishProtocol);
        Assert.isNull(this.publishProtocol);

        this.publishProtocol = publishProtocol;
    }

    @Override
    public IQueryClientProtocol getQueryProtocol() {
        return queryProtocol;
    }

    public void setQueryProtocol(IQueryClientProtocol queryProtocol) {
        Assert.notNull(queryProtocol);
        Assert.isNull(this.queryProtocol);

        this.queryProtocol = queryProtocol;
    }

    @Override
    public void start() {
        inProgress = false;

        if (clientStore != null) {
            clientStore.start();

            configuration = clientStore.loadGroupConfiguration(initialConfiguration.getGroupId());
            if (configuration == null) {
                configuration = initialConfiguration;
                clientStore.saveGroupConfiguration(configuration);
            }
        }

        queryProtocol.updateConfiguration(configuration);

        updateServerConfigurations();

        heartbeatTimer.start();
        queryProtocol.start();
    }

    @Override
    public void stop() {
        if (clientStore != null)
            clientStore.stop();

        onLeaderDisconnected();
        heartbeatTimer.stop();
        queryProtocol.stop();
    }

    @Override
    public void onTimer(long currentTime) {
        leaderTimeoutTimer.onTimer(currentTime);
        updateConfigurationTimer.onTimer(currentTime);
        heartbeatTimer.onTimer(currentTime);
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
    public GroupConfiguration getGroupConfiguration() {
        return configuration;
    }

    @Override
    public void sendRequest(PublishRequest request) {
        IMessageSender messageSender = ensureMessageSender();
        if (messageSender == null)
            return;

        if (logger.isLogEnabled(LogLevel.TRACE))
            logger.log(LogLevel.TRACE, marker, messages.requestSent(request));

        heartbeatTimer.restart();

        ByteArray serializedRequest = MessageUtils.write(request);
        inProgress = true;

        messageSender.send(serializedRequest);
    }

    @Override
    public void onLeaderDisconnected() {
        if (leaderId != null) {
            if (logger.isLogEnabled(LogLevel.INFO))
                logger.log(LogLevel.INFO, marker, messages.leaderDisconnected(leaderEndpoint, leaderId));

            leaderId = null;
            leaderEndpoint = null;
            inProgress = false;

            stopMessageSender();

            publishProtocol.onLeaderDisconnected();
            queryProtocol.onLeaderDisconnected();

            leaderTimeoutTimer.stop();
            updateConfigurationTimer.stop();
        }
    }

    private void handleSucceededResponse(PublishResponse response) {
        inProgress = false;

        if (!response.getSource().equals(leaderId))
            return;

        if (logger.isLogEnabled(LogLevel.TRACE))
            logger.log(LogLevel.TRACE, marker, messages.responseReceived(response));

        if (response.isAccepted()) {
            publishProtocol.handleAcceptedPublishResponse(response);

            if (response.getConfiguration() != null)
                updateConfiguration(response.getConfiguration());

            leaderTimeoutTimer.restart();
        } else if (response.getLeaderId() == null || !response.getLeaderId().equals(leaderId)) {
            if (logger.isLogEnabled(LogLevel.DEBUG))
                logger.log(LogLevel.DEBUG, marker, messages.endpointNotLeader(leaderEndpoint, leaderId));

            onLeaderDisconnected();

            if (response.getLeaderId() != null)
                leaderId = response.getLeaderId();
        }
    }

    private void handleFailedResponse(String endpoint, Throwable error) {
        inProgress = false;

        if (logger.isLogEnabled(LogLevel.WARNING))
            logger.log(LogLevel.WARNING, marker, messages.responseErrorReceived(endpoint), error);

        onLeaderDisconnected();
    }

    private void handleLeaderTimeout() {
        if (logger.isLogEnabled(LogLevel.WARNING))
            logger.log(LogLevel.WARNING, marker, messages.leaderTimeout(leaderEndpoint));

        onLeaderDisconnected();
    }

    private void onLeaderConnected() {
        inProgress = false;
        leaderTimeoutTimer.start();

        if (!configuration.isSingleServer())
            updateConfigurationTimer.start();

        queryProtocol.onLeaderConnected(messageSender, new ServerConfiguration(leaderId, leaderEndpoint));

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.leaderConnected(leaderEndpoint, leaderId));
    }

    private IMessageSender ensureMessageSender() {
        if (messageSender == null) {
            try {
                ServerConfiguration configuration = getLeaderConfiguration(leaderId);
                leaderId = configuration.getId();
                leaderEndpoint = configuration.getEndpoint();
                messageSender = messageSenderFactory.createSender(configuration.getEndpoint(), new CompletionHandler<ByteArray>() {
                    @Override
                    public void onSucceeded(ByteArray response) {
                        Message deserializedResponse = MessageUtils.read(response);

                        compartment.offer(() -> handleSucceededResponse((PublishResponse)deserializedResponse));
                    }

                    @Override
                    public void onFailed(Throwable error) {
                        compartment.offer(() -> handleFailedResponse(configuration.getEndpoint(), error));
                    }
                });
                messageSender.start();

                onLeaderConnected();
            } catch (Exception e) {
                if (logger.isLogEnabled(LogLevel.WARNING))
                    logger.log(LogLevel.WARNING, marker, e);

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

    private ServerConfiguration getLeaderConfiguration(UUID leaderId) {
        if (leaderId != null) {
            for (ServerConfiguration serverConfiguration : serverConfigurations) {
                if (serverConfiguration.getId().equals(leaderId))
                    return serverConfiguration;
            }
        }

        int index = random.nextInt(serverConfigurations.size());
        ServerConfiguration serverConfiguration = serverConfigurations.get(index);

        if (logger.isLogEnabled(LogLevel.DEBUG))
            logger.log(LogLevel.DEBUG, marker, messages.randomServerSelected(serverConfiguration));

        return serverConfiguration;
    }

    private void requestHeartbeat() {
        if (inProgress)
            return;

        PublishRequestBuilder request = new PublishRequestBuilder();
        request.setSource(clientId);
        request.setGroupId(groupId);

        sendRequest(request.toMessage());
    }

    private void requestConfigurationUpdate() {
        if (inProgress) {
            updateConfigurationTimer.delayedFire();
            return;
        }

        PublishRequestBuilder request = new PublishRequestBuilder();
        request.setSource(clientId);
        request.setGroupId(groupId);
        request.setFlags(Enums.of(MessageFlags.REQUEST_CONFIGURATION));

        sendRequest(request.toMessage());
    }

    private void updateConfiguration(GroupConfiguration configuration) {
        if (!configuration.equals(this.configuration)) {
            this.configuration = configuration;
            clientStore.saveGroupConfiguration(configuration);

            updateServerConfigurations();

            queryProtocol.updateConfiguration(configuration);

            if (logger.isLogEnabled(LogLevel.DEBUG))
                logger.log(LogLevel.DEBUG, marker, messages.configurationUpdated(configuration));
        }
    }

    private void updateServerConfigurations() {
        Map<UUID, ServerConfiguration> map = new LinkedHashMap<>();
        for (ServerConfiguration serverConfiguration : initialConfiguration.getServers())
            map.put(serverConfiguration.getId(), serverConfiguration);

        if (configuration != null) {
            for (ServerConfiguration serverConfiguration : configuration.getServers())
                map.put(serverConfiguration.getId(), serverConfiguration);
        }

        serverConfigurations = new ArrayList<>(map.values());
    }

    private interface IMessages {
        @DefaultMessage("Request sent: {0}.")
        ILocalizedMessage requestSent(ClientRequest request);

        @DefaultMessage("Response received: {0}.")
        ILocalizedMessage responseReceived(ClientResponse response);

        @DefaultMessage("Response error received from server ''{0}''.")
        ILocalizedMessage responseErrorReceived(String serverId);

        @DefaultMessage("Timeout has occured, when waiting response from leader ''{0}''.")
        ILocalizedMessage leaderTimeout(String leaderId);

        @DefaultMessage("Random server has been selected as primary group endpoint: {0}.")
        ILocalizedMessage randomServerSelected(ServerConfiguration serverConfiguration);

        @DefaultMessage("Group configuration has been updated: {0}.")
        ILocalizedMessage configurationUpdated(GroupConfiguration configuration);

        @DefaultMessage("Group endpoint has been connected: {0}:{1}.")
        ILocalizedMessage leaderConnected(String leaderEndpoint, UUID leaderId);

        @DefaultMessage("Group endpoint has been disconnected: {0}:{1}.")
        ILocalizedMessage leaderDisconnected(String leaderEndpoint, UUID leaderId);

        @DefaultMessage("Group endpoint is not a leader: {0}:{1}.")
        ILocalizedMessage endpointNotLeader(String leaderEndpoint, UUID leaderId);
    }
}
