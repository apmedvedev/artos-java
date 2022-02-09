package com.artos.inttests.core.sim.impl;

import com.artos.api.core.server.IServerChannel;
import com.artos.api.core.server.IServerChannelFactory;
import com.artos.api.core.server.StopType;
import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.api.core.server.conf.ServerChannelConfigurationBuilder;
import com.artos.api.core.server.conf.ServerChannelFactoryConfigurationBuilder;
import com.artos.api.core.server.conf.ServerConfiguration;
import com.artos.impl.core.server.impl.ServerRole;
import com.artos.inttests.core.sim.api.ISimGroupServers;
import com.artos.inttests.core.sim.model.SimLink;
import com.artos.inttests.core.sim.model.SimListenerFactoryConfiguration;
import com.artos.inttests.core.sim.model.SimLogStoreFactoryConfiguration;
import com.artos.inttests.core.sim.model.SimPublishDriver;
import com.artos.inttests.core.sim.model.SimSenderFactoryConfiguration;
import com.artos.inttests.core.sim.model.SimServer;
import com.artos.inttests.core.sim.model.SimStateMachine;
import com.artos.inttests.core.sim.model.SimStateMachineFactory;
import com.artos.inttests.core.sim.perf.IMeter;
import com.artos.inttests.core.sim.perf.PerfRegistry;
import com.exametrika.common.l10n.DefaultMessage;
import com.exametrika.common.l10n.ILocalizedMessage;
import com.exametrika.common.l10n.Messages;
import com.exametrika.common.log.ILogger;
import com.exametrika.common.log.IMarker;
import com.exametrika.common.log.LogLevel;
import com.exametrika.common.log.Loggers;
import com.exametrika.common.utils.Files;
import com.exametrika.common.utils.Threads;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class SimGroupServers implements ISimGroupServers {
    private static final IMessages messages = Messages.get(IMessages.class);
    private static final int LOG_STORE_MIN_RETENTION_PERIOD = 60000;
    private static final int STATE_TRANSFER_PORT_RANGE_START = 17000;
    private static final int STATE_TRANSFER_PORT_RANGE_END = 19000;
    private static final String STATE_TRANSFER_BIND_ADDRESS = "localhost";
    private static final boolean STATE_TRANSFER_USE_COMPRESSION = true;
    private static final int STATE_TRANSFER_MAX_SIZE = 10000000;
    private final ILogger logger = Loggers.get(getClass());
    private final IMarker marker = Loggers.getMarker("model");
    private final List<SimServer> servers = new ArrayList<>();
    private final List<SimStateMachine> removedServers = new ArrayList<>();
    private final Random random = new Random();
    private final GroupConfiguration initialGroupConfiguration;
    private int nextServerId = 1;
    private final String groupName;
    private final int serverCount;
    private final boolean enableStateMachineChecks;
    private final long subscriptionEventPeriod;
    private final int subscriptionEventBatchSize;
    private final boolean singleServer;
    private final IMeter serversMeter;
    private final IMeter failedServersMeter;
    private final IMeter leftServersMeter;
    private final IMeter joinedServersMeter;
    private final IMeter restoredServersMeter;
    private final PerfRegistry perfRegistry;
    private final SimGroupLinks links;
    private final SimGroupDrivers drivers;
    private File workDir;

    public SimGroupServers(String groupName, int serverCount, PerfRegistry perfRegistry, SimGroupLinks links, SimGroupDrivers drivers,
                           boolean enableStateMachineChecks, long subscriptionEventPeriod, int subscriptionEventBatchSize,
                           boolean singleServer) {
        this.groupName = groupName;
        this.serverCount = serverCount;
        this.enableStateMachineChecks = enableStateMachineChecks;
        this.subscriptionEventPeriod = subscriptionEventPeriod;
        this.subscriptionEventBatchSize = subscriptionEventBatchSize;
        this.singleServer = singleServer;

        serversMeter = perfRegistry.getMeter("model:" + groupName + ".total.servers(counts)");
        failedServersMeter = perfRegistry.getMeter("model:" + groupName + ".total.failed-servers(counts)");
        leftServersMeter = perfRegistry.getMeter("model:" + groupName + ".total.left-servers(counts)");
        joinedServersMeter = perfRegistry.getMeter("model:" + groupName + ".total.joined-servers(counts)");
        restoredServersMeter = perfRegistry.getMeter("model:" + groupName + ".total.restored-servers(counts)");
        this.perfRegistry = perfRegistry;
        this.links = links;
        this.drivers = drivers;
        initialGroupConfiguration = createInitialGroupConfiguration();
    }

    public List<SimServer> getServers() {
        return new ArrayList<>(servers);
    }

    public GroupConfiguration getInitialGroupConfiguration() {
        return initialGroupConfiguration;
    }

    public synchronized void start() {
        workDir = Files.createTempDirectory("raft");

        for (int i = 0; i < serverCount; i++)
            addServer();

        serversMeter.measure(servers.size());
        Files.delete(workDir);
    }

    public synchronized void stop() {
        for (SimServer server : servers)
            server.stop(Integer.MAX_VALUE, StopType.IMMEDIATE);
    }

    public synchronized SimServer addServer() {
        int id = nextServerId;
        nextServerId++;
        ServerConfiguration configuration;
        if (id <= initialGroupConfiguration.getServers().size())
            configuration = initialGroupConfiguration.getServers().get(id - 1);
        else
            configuration = new ServerConfiguration(UUID.randomUUID(), getServerEndpointName(id));

        SimLink link = links.addLink(configuration.getEndpoint());

        SimPublishDriver driver = drivers.addPublishDriver(configuration.getEndpoint(), false);

        SimStateMachineFactory stateMachineFactory = new SimStateMachineFactory(configuration.getId(), configuration.getEndpoint(),
            perfRegistry, driver, enableStateMachineChecks, subscriptionEventPeriod, subscriptionEventBatchSize);

        ServerChannelFactoryConfigurationBuilder factoryConfigurationBuilder = new ServerChannelFactoryConfigurationBuilder();
        IServerChannelFactory factory = factoryConfigurationBuilder.toConfiguration().createFactory();

        File workDir = Files.createTempDirectory(this.workDir, "raft");
        ServerChannelConfigurationBuilder configurationBuilder = new ServerChannelConfigurationBuilder()
            .setServer(configuration)
            .setGroup(initialGroupConfiguration)
            .setLogStore(new SimLogStoreFactoryConfiguration(configuration.getEndpoint(), perfRegistry, LOG_STORE_MIN_RETENTION_PERIOD))
            .setMessageListener(new SimListenerFactoryConfiguration(link, perfRegistry))
            .setMessageSender(new SimSenderFactoryConfiguration(link, perfRegistry))
            .setStateTransferPortRangeStart(STATE_TRANSFER_PORT_RANGE_START)
            .setStateTransferPortRangeEnd(STATE_TRANSFER_PORT_RANGE_END)
            .setStateTransferBindAddress(STATE_TRANSFER_BIND_ADDRESS)
            .setStateTransferUseCompression(STATE_TRANSFER_USE_COMPRESSION)
            .setStateTransferMaxSize(STATE_TRANSFER_MAX_SIZE)
            .setWorkDir(workDir.getAbsolutePath());

        IServerChannel serverChannel = factory.createChannel(configurationBuilder.toConfiguration(), stateMachineFactory);

        SimServer server = new SimServer(configuration.getEndpoint(), serverChannel);
        servers.add(server);

        server.start();

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.serverAdded(configuration.getEndpoint()));

        return server;
    }

    @Override
    public synchronized void joinServer() {
        SimServer server = addServer();

        Threads.sleep(5000);

        server.getStateMachine().getDriver().setEnabled(true);

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.serverJoined(server.getEndpoint()));

        joinedServersMeter.measure(1);
        serversMeter.measure(servers.size());
    }

    @Override
    public synchronized void failServer(boolean leader) {
        for (int i = 0; i < servers.size(); i++) {
            SimServer server = servers.get(i);
            if (server.getRole() == getServerRole(leader)) {
                removeServer(i, StopType.IMMEDIATE);
                break;
            }
        }
    }

    @Override
    public synchronized void gracefullyStopServer(boolean leader) {
        for (int i = 0; i < servers.size(); i++) {
            SimServer server = servers.get(i);
            if (server.getRole() == getServerRole(leader)) {
                removeServer(i, StopType.GRACEFULLY);
                break;
            }
        }
    }

    @Override
    public synchronized void failRandomServer() {
        int index = random.nextInt(servers.size());
        removeServer(index, StopType.IMMEDIATE);
    }

    @Override
    public synchronized void leaveServer(boolean leader) {
        for (int i = 0; i < servers.size(); i++) {
            SimServer server = servers.get(i);
            if (server.getRole() == getServerRole(leader)) {
                leaveServer(i);
                break;
            }
        }
    }

    @Override
    public synchronized void leaveRandomServer() {
        int index = random.nextInt(servers.size());
        leaveServer(index);
    }

    @Override
    public synchronized void restoreServer() {
        if (removedServers.isEmpty())
            return;

        SimStateMachine stateMachine = removedServers.remove(removedServers.size() - 1);
        ServerConfiguration configuration = new ServerConfiguration(stateMachine.getServerId(), stateMachine.getEndpoint());

        SimLink link = links.addLink(configuration.getEndpoint());

        drivers.addPublishDriver(stateMachine.getDriver());

        ServerChannelFactoryConfigurationBuilder factoryConfigurationBuilder = new ServerChannelFactoryConfigurationBuilder();
        IServerChannelFactory factory = factoryConfigurationBuilder.toConfiguration().createFactory();

        File workDir = Files.createTempDirectory(this.workDir, "raft");
        ServerChannelConfigurationBuilder configurationBuilder = new ServerChannelConfigurationBuilder()
            .setServer(configuration)
            .setGroup(initialGroupConfiguration)
            .setLogStore(new SimLogStoreFactoryConfiguration(configuration.getEndpoint(), perfRegistry, LOG_STORE_MIN_RETENTION_PERIOD))
            .setMessageListener(new SimListenerFactoryConfiguration(link, perfRegistry))
            .setMessageSender(new SimSenderFactoryConfiguration(link, perfRegistry))
            .setStateTransferPortRangeStart(STATE_TRANSFER_PORT_RANGE_START)
            .setStateTransferPortRangeEnd(STATE_TRANSFER_PORT_RANGE_END)
            .setStateTransferBindAddress(STATE_TRANSFER_BIND_ADDRESS)
            .setStateTransferUseCompression(STATE_TRANSFER_USE_COMPRESSION)
            .setStateTransferMaxSize(STATE_TRANSFER_MAX_SIZE)
            .setWorkDir(workDir.getAbsolutePath());

        IServerChannel serverChannel = factory.createChannel(configurationBuilder.toConfiguration(),
            new SimStateMachineFactory(stateMachine));

        SimServer server = new SimServer(configuration.getEndpoint(), serverChannel);
        servers.add(server);

        server.start();

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.serverRestored(configuration.getEndpoint()));

        restoredServersMeter.measure(1);

        Threads.sleep(5000);

        server.getStateMachine().getDriver().setEnabled(true);
    }

    @Override
    public synchronized void disableLink(boolean leader) {
        for (int i = 0; i < servers.size(); i++) {
            SimServer server = servers.get(i);
            if (server.getRole() == getServerRole(leader)) {
                links.disableLink(server.getEndpoint());
                break;
            }
        }
    }

    private synchronized void removeServer(int index, StopType stopType) {
        SimServer server = servers.remove(index);
        drivers.removePublishDriver(server.getEndpoint());
        server.stop(Integer.MAX_VALUE, stopType);

        links.removeLink(server.getEndpoint());

        removedServers.add(server.getStateMachine());

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.serverRemoved(server.getEndpoint()));

        failedServersMeter.measure(1);
        serversMeter.measure(servers.size());
    }

    private synchronized void leaveServer(int index) {
        SimServer server = servers.get(index);
        removeServer(index, StopType.LEAVE_GROUP);

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.serverLeft(server.getEndpoint()));

        leftServersMeter.measure(1);
    }

    private ServerRole getServerRole(boolean leader) {
        return leader ? ServerRole.LEADER : ServerRole.FOLLOWER;
    }

    private GroupConfiguration createInitialGroupConfiguration() {
        List<ServerConfiguration> servers = new ArrayList<>();
        for (int i = 1; i <= serverCount; i++) {
            ServerConfiguration serverConfiguration = new ServerConfiguration(UUID.randomUUID(), getServerEndpointName(i));
            servers.add(serverConfiguration);
        }

        GroupConfiguration initialGroupConfiguration = new GroupConfiguration(groupName, UUID.randomUUID(), servers, singleServer);

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.initialConfiguration(initialGroupConfiguration));

        return initialGroupConfiguration;
    }

    private String getServerEndpointName(int id) {
        return groupName + ".server-" + id;
    }

    private interface IMessages {
        @DefaultMessage("Server has been added: {0}.")
        ILocalizedMessage serverAdded(String endpoint);

        @DefaultMessage("Server has joined: {0}.")
        ILocalizedMessage serverJoined(String endpoint);

        @DefaultMessage("Server has been restored: {0}.")
        ILocalizedMessage serverRestored(String endpoint);

        @DefaultMessage("Server has been removed: {0}.")
        ILocalizedMessage serverRemoved(String endpoint);

        @DefaultMessage("Server has left: {0}.")
        ILocalizedMessage serverLeft(String endpoint);

        @DefaultMessage("Initial group configuration: {0}.")
        ILocalizedMessage initialConfiguration(GroupConfiguration initialGroupConfiguration);
    }
}
