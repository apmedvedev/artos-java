package com.artos.tests.core;

import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.api.core.server.conf.MemoryLogStoreFactoryConfiguration;
import com.artos.api.core.server.conf.ServerChannelConfiguration;
import com.artos.api.core.server.conf.ServerChannelConfigurationBuilder;
import com.artos.api.core.server.conf.ServerChannelFactoryConfiguration;
import com.artos.api.core.server.conf.ServerChannelFactoryConfigurationBuilder;
import com.artos.api.core.server.conf.ServerConfiguration;
import com.artos.impl.core.message.AppendEntriesRequestBuilder;
import com.artos.impl.core.message.AppendEntriesResponse;
import com.artos.impl.core.message.AppendEntriesResponseBuilder;
import com.artos.impl.core.message.PublishRequestBuilder;
import com.artos.impl.core.server.impl.Context;
import com.artos.impl.core.server.impl.State;
import com.artos.spi.core.ILogStore;
import com.artos.spi.core.IMessageListenerFactory;
import com.artos.spi.core.IMessageSenderFactory;
import com.artos.spi.core.IStateMachine;
import com.artos.spi.core.LogEntry;
import com.artos.spi.core.LogValueType;
import com.artos.spi.core.ServerState;
import com.artos.tests.core.mocks.LogStoreMock;
import com.artos.tests.core.mocks.MessageListenerFactoryMock;
import com.artos.tests.core.mocks.MessageSenderFactoryMock;
import com.artos.tests.core.mocks.StateMachineMock;
import com.artos.tests.core.mocks.TestMessageListenerFactoryConfiguration;
import com.artos.tests.core.mocks.TestMessageSenderFactoryConfiguration;
import com.exametrika.common.compartment.ICompartment;
import com.exametrika.common.compartment.ICompartmentFactory.Parameters;
import com.exametrika.common.compartment.impl.CompartmentFactory;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.Times;
import org.junit.After;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public abstract class AbstractRaftTest {
    protected Context context;
    protected State state;

    @After
    public void tearDown() {
        Times.clearTest();

        if (context != null)
            context.getCompartment().stop();
    }

    protected State createState(int serverCount) {
        State state = new State();

        List<ServerConfiguration> serverConfigurations = new ArrayList<>();
        for (int i = 0; i < serverCount; i++)
            serverConfigurations.add(new ServerConfiguration(UUID.randomUUID(), "testEndpoint-" + i));

        GroupConfiguration groupConfiguration = new GroupConfiguration("test", UUID.randomUUID(), serverConfigurations, false);
        state.setConfiguration(groupConfiguration);

        ServerState serverState = new ServerState();
        state.setServerState(serverState);
        this.state = state;
        return  state;
    }

    protected Context createContext(GroupConfiguration groupConfiguration) {
        IStateMachine stateMachine = new StateMachineMock();
        stateMachine.beginTransaction(false).writeConfiguration(groupConfiguration);
        IMessageListenerFactory messageListenerFactory = new MessageListenerFactoryMock();
        IMessageSenderFactory messageSenderFactory = new MessageSenderFactoryMock();

        ICompartment compartment = new CompartmentFactory().createCompartment(new Parameters());
        compartment.start();

        ServerChannelFactoryConfiguration serverChannelFactoryConfiguration =
            new ServerChannelFactoryConfigurationBuilder().toConfiguration();
        ServerChannelConfiguration serverChannelConfiguration = new ServerChannelConfigurationBuilder()
            .setGroup(groupConfiguration)
            .setServer(groupConfiguration.getServers().get(0))
            .setLogStore(new MemoryLogStoreFactoryConfiguration(0))
            .setMessageListener(new TestMessageListenerFactoryConfiguration())
            .setMessageSender(new TestMessageSenderFactoryConfiguration())
            .setStateTransferPortRangeStart(17000)
            .setStateTransferPortRangeEnd(19000)
            .setStateTransferUseCompression(true)
            .setStateTransferMaxSize(1000)
            .setStateTransferBindAddress("localhost")
            .setWorkDir("").toConfiguration();

        ILogStore logStore = new LogStoreMock(0);

        ServerConfiguration configuration = groupConfiguration.getServers().get(0);
        return new Context(configuration, groupConfiguration.getGroupId(), logStore,
            stateMachine, serverChannelFactoryConfiguration, serverChannelConfiguration, messageListenerFactory, messageSenderFactory, compartment);
    }

    protected AppendEntriesRequestBuilder createAppendEntriesRequest() {
        AppendEntriesRequestBuilder builder = new AppendEntriesRequestBuilder();
        builder.setGroupId(context.getGroupId());
        if (state.getConfiguration().getServers().size() > 1)
            builder.setSource(state.getConfiguration().getServers().get(1).getId());
        else
            builder.setSource(UUID.randomUUID());
        builder.setLogEntries(new ArrayList<>());
        return builder;
    }

    protected AppendEntriesResponse createAppendEntriesResponse() {
        AppendEntriesResponseBuilder builder = new AppendEntriesResponseBuilder();
        builder.setGroupId(UUID.randomUUID());
        builder.setSource(UUID.randomUUID());
        return builder.toMessage();
    }

    protected PublishRequestBuilder createPublishRequest() {
        PublishRequestBuilder request = new PublishRequestBuilder();
        request.setGroupId(context.getGroupId());
        request.setSource(UUID.randomUUID());
        return request;
    }

    protected void addLogEntries(UUID clientId, long term, int count) {
        for (int i = 0; i < count; i++)
            context.getLogStore().append(new LogEntry(term, new ByteArray(new byte[0]), LogValueType.APPLICATION,
                clientId, context.getLogStore().getEndIndex()));
    }

    protected List<LogEntry> createLogEntries(UUID clientId, long term, int start, int count) {
        List<LogEntry> logEntries = new ArrayList<>();
        for (int i = 0; i < count; i++)
            logEntries.add(new LogEntry(term, new ByteArray(new byte[0]), LogValueType.APPLICATION, clientId, start + i));

        return logEntries;
    }

    protected List<ByteArray> createLogEntries(int count, int size) {
        List<ByteArray> list = new ArrayList<>();
        for (int i = 0; i < count; i++)
            list.add(new ByteArray(new byte[size]));

        return list;
    }

    protected void checkLogEntries(List<LogEntry> logEntries, UUID clientId, long term, long startIndex, int count, boolean checkValue) {
        assertThat(logEntries.size(), is(count));
        for (int i = 0; i < count; i++) {
            LogEntry entry = logEntries.get(i);
            assertThat(entry.getValueType(), is(LogValueType.APPLICATION));
            assertThat(entry.getClientId(), is(clientId));
            assertThat(entry.getMessageId(), is(startIndex + i));
            assertThat(entry.getTerm(), is(term));
            if (checkValue)
                assertThat(entry.getValue() != null, is(true));
        }
    }

    protected ByteArray createBuffer(byte start, int size) {
        byte[] buffer = new byte[size];
        byte value = start;
        for (int i = 0; i < size; i++) {
            buffer[i] = value;
            value++;
        }

        return new ByteArray(buffer);
    }
}
