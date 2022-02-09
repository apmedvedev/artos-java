/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.server.impl;

import com.artos.api.core.server.conf.ServerChannelConfiguration;
import com.artos.api.core.server.conf.ServerChannelFactoryConfiguration;
import com.artos.api.core.server.conf.ServerConfiguration;
import com.artos.spi.core.ILogStore;
import com.artos.spi.core.IMessageListenerFactory;
import com.artos.spi.core.IMessageSenderFactory;
import com.artos.spi.core.IStateMachine;
import com.exametrika.common.compartment.ICompartment;
import com.exametrika.common.log.IMarker;
import com.exametrika.common.log.Loggers;
import com.exametrika.common.utils.Assert;

import java.util.UUID;

public class Context {
    private final ServerConfiguration localServer;
    private final UUID groupId;
    private final IMessageListenerFactory messageListenerFactory;
    private final IMessageSenderFactory messageSenderFactory;
    private final ILogStore logStore;
    private final IStateMachine stateMachine;
    private final ServerChannelFactoryConfiguration serverChannelFactoryConfiguration;
    private final ServerChannelConfiguration serverChannelConfiguration;
    private final ICompartment compartment;
    private final IMarker marker;

    public Context(ServerConfiguration localServer, UUID groupId, ILogStore logStore, IStateMachine stateMachine,
                   ServerChannelFactoryConfiguration serverChannelFactoryConfiguration,
                   ServerChannelConfiguration serverChannelConfiguration, IMessageListenerFactory messageListenerFactory,
                   IMessageSenderFactory messageSenderFactory, ICompartment compartment) {
        Assert.notNull(localServer);
        Assert.notNull(groupId);
        Assert.notNull(logStore);
        Assert.notNull(stateMachine);
        Assert.notNull(messageListenerFactory);
        Assert.notNull(messageSenderFactory);
        Assert.notNull(compartment);
        Assert.notNull(serverChannelFactoryConfiguration);
        Assert.notNull(serverChannelConfiguration);

        this.localServer = localServer;
        this.groupId = groupId;
        this.logStore = logStore;
        this.stateMachine = stateMachine;
        this.messageSenderFactory = messageSenderFactory;
        this.messageListenerFactory = messageListenerFactory;
        this.compartment = compartment;
        this.serverChannelFactoryConfiguration = serverChannelFactoryConfiguration;
        this.serverChannelConfiguration = serverChannelConfiguration;
        this.marker = Loggers.getMarker(localServer.getEndpoint());
    }

    public ServerConfiguration getLocalServer() {
        return localServer;
    }

    public UUID getServerId() {
        return localServer.getId();
    }

    public UUID getGroupId() {
        return groupId;
    }

    public IMarker getMarker() {
        return marker;
    }

    public IMessageListenerFactory getMessageListenerFactory() {
        return messageListenerFactory;
    }

    public IMessageSenderFactory getMessageSenderFactory() {
        return messageSenderFactory;
    }

    public ILogStore getLogStore() {
        return logStore;
    }

    public IStateMachine getStateMachine() {
        return stateMachine;
    }

    public ServerChannelFactoryConfiguration getServerChannelFactoryConfiguration() {
        return serverChannelFactoryConfiguration;
    }

    public ServerChannelConfiguration getServerChannelConfiguration() {
        return serverChannelConfiguration;
    }

    public ICompartment getCompartment() {
        return compartment;
    }

    @Override
    public String toString() {
        return localServer.toString();
    }
}
