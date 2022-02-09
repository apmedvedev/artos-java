package com.artos.inttests.shards.sim.impl;

import com.artos.api.core.client.conf.ClientChannelFactoryConfigurationBuilder;
import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.api.shards.client.IDataSpaceClientChannel;
import com.artos.api.shards.client.IDataSpaceClientChannelFactory;
import com.artos.api.shards.client.conf.StaticDataSpaceClientChannelConfigurationBuilder;
import com.artos.impl.shards.client.channel.StaticDataSpaceClientChannelFactory;
import com.artos.inttests.core.sim.impl.SimGroupLinks;
import com.artos.inttests.core.sim.impl.SimGroupServers;
import com.artos.inttests.core.sim.model.SimClientStore;
import com.artos.inttests.core.sim.model.SimClientStoreConfiguration;
import com.artos.inttests.core.sim.model.SimFlowController;
import com.artos.inttests.core.sim.model.SimLink;
import com.artos.inttests.core.sim.model.SimSenderFactoryConfiguration;
import com.artos.inttests.core.sim.perf.IMeter;
import com.artos.inttests.core.sim.perf.PerfRegistry;
import com.artos.inttests.shards.sim.api.ISimDataSpaceClients;
import com.artos.inttests.shards.sim.model.SimDataSpaceClient;
import com.artos.inttests.shards.sim.model.SimDataSpacePublishDriver;
import com.artos.inttests.shards.sim.model.SimDataSpaceQueryDriver;
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

public class SimDataSpaceClients implements ISimDataSpaceClients {
    private static final IMessages messages = Messages.get(IMessages.class);
    private final ILogger logger = Loggers.get(getClass());
    private final IMarker marker = Loggers.getMarker("model");
    private final Random random = new Random();
    private int nextClientId = 1;
    private final List<SimDataSpaceClient> clients = new ArrayList<>();
    private final int clientCount;
    private final IMeter clientsMeter;
    private final SimGroupLinks links;
    private final List<SimGroupServers> serverGroups;
    private final PerfRegistry perfRegistry;
    private final SimDataSpaceDrivers drivers;

    public SimDataSpaceClients(int clientCount, PerfRegistry perfRegistry, SimDataSpaceDrivers drivers, List<SimGroupServers> serverGroups,
                               SimGroupLinks links) {
        this.clientCount = clientCount;
        this.perfRegistry = perfRegistry;
        this.drivers = drivers;
        this.serverGroups = serverGroups;
        this.links = links;
        clientsMeter = perfRegistry.getMeter("model:total.clients(counts)");
    }

    public synchronized SimDataSpaceClient addClient() {
        int id = nextClientId;
        nextClientId++;
        String endpoint = getClientEndpointName(id);

        SimLink link = links.addLink(endpoint);
        SimDataSpacePublishDriver publishDriver = (SimDataSpacePublishDriver) drivers.addPublishDriver(endpoint, true);
        SimDataSpaceQueryDriver queryDriver = (SimDataSpaceQueryDriver) drivers.addQueryDriver(endpoint, publishDriver);

        ClientChannelFactoryConfigurationBuilder factoryConfigurationBuilder = new ClientChannelFactoryConfigurationBuilder();
        IDataSpaceClientChannelFactory factory = new StaticDataSpaceClientChannelFactory(factoryConfigurationBuilder.toConfiguration());

        SimClientStore clientStore = new SimClientStore();

        List<GroupConfiguration> groups = new ArrayList<>();
        for (SimGroupServers serverGroup : serverGroups)
            groups.add(serverGroup.getInitialGroupConfiguration());

        StaticDataSpaceClientChannelConfigurationBuilder configurationBuilder = new StaticDataSpaceClientChannelConfigurationBuilder()
            .setGroups(groups)
            .setMessageSender(new SimSenderFactoryConfiguration(link, perfRegistry))
            .setClientStore(new SimClientStoreConfiguration(clientStore))
            .setEndpoint(endpoint);

        IDataSpaceClientChannel clientChannel = factory.createChannel(configurationBuilder.toConfiguration(),
            new SimFlowController(publishDriver, queryDriver));

        SimDataSpaceClient client = new SimDataSpaceClient(endpoint, clientChannel);
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
        SimDataSpaceClient client = addClient();

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
        SimDataSpaceClient client = clients.remove(index);
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
        for (SimDataSpaceClient client : clients)
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
