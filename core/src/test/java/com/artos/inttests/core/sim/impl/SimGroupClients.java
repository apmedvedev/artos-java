package com.artos.inttests.core.sim.impl;

import com.artos.api.core.client.IClientChannel;
import com.artos.api.core.client.IClientChannelFactory;
import com.artos.api.core.client.conf.ClientChannelConfigurationBuilder;
import com.artos.api.core.client.conf.ClientChannelFactoryConfigurationBuilder;
import com.artos.inttests.core.sim.api.ISimGroupClients;
import com.artos.inttests.core.sim.model.SimClient;
import com.artos.inttests.core.sim.model.SimClientStore;
import com.artos.inttests.core.sim.model.SimClientStoreConfiguration;
import com.artos.inttests.core.sim.model.SimFlowController;
import com.artos.inttests.core.sim.model.SimLink;
import com.artos.inttests.core.sim.model.SimPublishDriver;
import com.artos.inttests.core.sim.model.SimQueryDriver;
import com.artos.inttests.core.sim.model.SimSenderFactoryConfiguration;
import com.artos.inttests.core.sim.perf.IMeter;
import com.artos.inttests.core.sim.perf.PerfRegistry;
import com.exametrika.common.l10n.DefaultMessage;
import com.exametrika.common.l10n.ILocalizedMessage;
import com.exametrika.common.l10n.Messages;
import com.exametrika.common.log.ILogger;
import com.exametrika.common.log.IMarker;
import com.exametrika.common.log.LogLevel;
import com.exametrika.common.log.Loggers;
import com.exametrika.common.utils.Threads;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SimGroupClients implements ISimGroupClients {
    private static final IMessages messages = Messages.get(IMessages.class);
    private final ILogger logger = Loggers.get(getClass());
    private final IMarker marker = Loggers.getMarker("model");
    private final Random random = new Random();
    private int nextClientId = 1;
    private final List<SimClient> clients = new ArrayList<>();
    private final int clientCount;
    private final IMeter clientsMeter;
    private final SimGroupLinks links;
    private final SimGroupServers servers;
    private final PerfRegistry perfRegistry;
    private final SimGroupDrivers drivers;

    public SimGroupClients(int clientCount, PerfRegistry perfRegistry, SimGroupDrivers drivers, SimGroupServers servers,
                           SimGroupLinks links) {
        this.clientCount = clientCount;
        this.perfRegistry = perfRegistry;
        this.drivers = drivers;
        this.servers = servers;
        this.links = links;
        clientsMeter = perfRegistry.getMeter("model:total.clients(counts)");
    }

    public synchronized SimClient addClient() {
        int id = nextClientId;
        nextClientId++;
        String endpoint = getClientEndpointName(id);

        SimLink link = links.addLink(endpoint);
        SimPublishDriver publishDriver = drivers.addPublishDriver(endpoint, true);
        SimQueryDriver queryDriver = drivers.addQueryDriver(endpoint, publishDriver);

        ClientChannelFactoryConfigurationBuilder factoryConfigurationBuilder = new ClientChannelFactoryConfigurationBuilder();
        IClientChannelFactory factory = factoryConfigurationBuilder.toConfiguration().createFactory();

        SimClientStore clientStore = new SimClientStore();

        ClientChannelConfigurationBuilder configurationBuilder = new ClientChannelConfigurationBuilder()
            .setEndpoint(endpoint)
            .setGroup(servers.getInitialGroupConfiguration())
            .setMessageSender(new SimSenderFactoryConfiguration(link, perfRegistry))
            .setClientStore(new SimClientStoreConfiguration(clientStore));

        IClientChannel clientChannel = factory.createChannel(configurationBuilder.toConfiguration(),
            new SimFlowController(publishDriver, queryDriver));

        SimClient client = new SimClient(endpoint, clientChannel);
        client.setClientStore(clientStore);
        clients.add(client);

        publishDriver.setChannel(client);
        queryDriver.setChannel(client);

        client.start();

        publishDriver.start();
        queryDriver.start();

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.clientAdded(endpoint));

        return client;
    }

    @Override
    public synchronized void joinClient() {
        SimClient client = addClient();

        Threads.sleep(5000);

        drivers.getPublishDriver(client.getEndpoint()).setEnabled(true);
        drivers.getQueryDriver(client.getEndpoint()).setEnabled(true);

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.clientJoined(client.getEndpoint()));

        clientsMeter.measure(clients.size());
    }

    @Override
    public synchronized void removeRandomClient() {
        int index = random.nextInt(clients.size());
        removeClient(index);
    }

    private synchronized void removeClient(int index) {
        SimClient client = clients.remove(index);
        drivers.removePublishDriver(client.getEndpoint());
        drivers.removeQueryDriver(client.getEndpoint());
        client.stop();

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.clientRemoved(client.getEndpoint()));

        clientsMeter.measure(clients.size());
    }

    public synchronized void start() {
        for (int i = 0; i < clientCount; i++)
            addClient();

        clientsMeter.measure(clients.size());
    }

    public synchronized void stop() {
        for (SimClient client : clients)
            client.stop();
    }

    private String getClientEndpointName(int id) {
        return "client-" + id;
    }

    private interface IMessages {
        @DefaultMessage("Client has been added: {0}.")
        ILocalizedMessage clientAdded(String endpoint);

        @DefaultMessage("Client has joined: {0}.")
        ILocalizedMessage clientJoined(String endpoint);

        @DefaultMessage("Client has been removed: {0}.")
        ILocalizedMessage clientRemoved(String endpoint);
    }
}
