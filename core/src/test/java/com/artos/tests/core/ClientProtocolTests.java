package com.artos.tests.core;

public class ClientProtocolTests extends AbstractRaftTest {
// TODO: исправить

//    private ClientProtocol clientProtocol;
//    private GroupConfiguration initialConfiguration;
//    private FlowControllerMock flowController;
//    private MessageSenderFactoryMock messageSenderFactory;
//    private CompartmentMock compartment;
//    private ClientStoreMock clientStore;
//    private MessageSenderMock messageSender;
//    private UUID clientId;
//
//    @Before
//    public void setUp() {
//        Times.setTest(0);
//        initialConfiguration = createState(10).getConfiguration();
//
//        clientId = UUID.randomUUID();
//        flowController = new FlowControllerMock();
//        messageSenderFactory = new MessageSenderFactoryMock();
//        compartment = new CompartmentMock();
//        clientStore = new ClientStoreMock();
//        clientProtocol = new ClientProtocol(messageSenderFactory, initialConfiguration, "testEndpoint",
//            clientId, initialConfiguration.getGroupId(), 1000, 2000, 20, 10, 3000, 100, 1000, clientStore);
//        clientProtocol.setFlowController(flowController);
//        clientProtocol.setCompartment(compartment);
//
//        messageSender = messageSenderFactory.getSender();
//
//        clientProtocol.start();
//    }
//
//    @After
//    @Override
//    public void tearDown() {
//        super.tearDown();
//
//        clientProtocol.stop();
//    }
//
//    @Test
//    public void testSaveLoadConfiguration() throws Throwable {
//        assertThat(clientStore.getConfiguration() == initialConfiguration, is(true));
//        assertThat(Tests.get(clientProtocol, "initialConfiguration") == initialConfiguration, is(true));
//        assertThat(Tests.get(clientProtocol, "configuration") == initialConfiguration, is(true));
//        List<ServerConfiguration> serverConfigurations = Tests.get(clientProtocol, "serverConfigurations");
//        assertThat(serverConfigurations, is(initialConfiguration.getServers()));
//
//        List<ServerConfiguration> list = new ArrayList<>();
//        for (int i = 0; i < 10; i++) {
//            if (i < 5)
//                list.add(new ServerConfiguration(UUID.randomUUID(), "testNewEndpoint-" + i));
//            else
//                list.add(new ServerConfiguration(serverConfigurations.get(i).getId(), "testModifiedEndpoint-" + i));
//        }
//
//        GroupConfiguration configuration = new GroupConfiguration("test", UUID.randomUUID(), list);
//
//        clientStore.setConfiguration(configuration);
//        clientProtocol = new ClientProtocol(messageSenderFactory, initialConfiguration, "testEndpoint",
//                clientId, initialConfiguration.getGroupId(), 1000, 2000, 20, 10, 3000, 100, 1000, clientStore);
//        clientProtocol.start();
//
//        assertThat(Tests.get(clientProtocol, "initialConfiguration") == initialConfiguration, is(true));
//        assertThat(Tests.get(clientProtocol, "configuration") == configuration, is(true));
//        serverConfigurations = Tests.get(clientProtocol, "serverConfigurations");
//        list = new ArrayList<>();
//        for (int i = 0; i < 15; i++) {
//            if (i < 5)
//                list.add(initialConfiguration.getServers().get(i));
//            else if (i < 10)
//                list.add(configuration.getServers().get(i));
//            else
//                list.add(configuration.getServers().get(i - 10));
//        }
//        assertThat(serverConfigurations, is(list));
//    }
//
//    @Test
//    public void testAppendEntries() throws Throwable {
//        CompletionHandlerMock commitHandler = new CompletionHandlerMock();
//        new Expected(IllegalArgumentException.class, (Runnable) () -> clientProtocol.publish(
//            new ByteArray(new byte[101]), new CompletionHandlerMock()));
//
//        SimpleDeque queue = Tests.get(clientProtocol, "queue");
//        clientProtocol.publish(new ByteArray(new byte[10]), commitHandler);
//        clientProtocol.publish(new ByteArray(new byte[20]), commitHandler);
//        checkLogEntries(toList(queue), clientId, 0, 1, 2, true);
//
//        clientProtocol.publish(new ByteArray(new byte[10]), commitHandler);
//        clientProtocol.publish(new ByteArray(new byte[20]), commitHandler);
//        checkLogEntries(toList(queue), clientId, 0, 1, 4, true);
//    }
//
//    @Test
//    public void testCreateRequest() throws Throwable {
//        UUID leaderId = UUID.randomUUID();
//        Tests.set(clientProtocol, "leaderId", leaderId);
//        assertThat(Tests.invoke(clientProtocol, "createRequest"), nullValue());
//
//        CompletionHandlerMock commitHandler = new CompletionHandlerMock();
//        clientProtocol.publish(new ByteArray(new byte[10]), commitHandler);
//        clientProtocol.publish(new ByteArray(new byte[20]), commitHandler);
//        PublishRequest request = Tests.invoke(clientProtocol, "createRequest");
//        assertThat(request.getGroupId(), is(initialConfiguration.getGroupId()));
//        assertThat(request.getSource(), is(clientId));
//        checkLogEntries(request.getLogEntries(), clientId, 0, 1, 2, true);
//
//        assertThat(Tests.invoke(clientProtocol, "createRequest"), nullValue());
//
//        clientProtocol.publish(new ByteArray(new byte[10]), commitHandler);
//        clientProtocol.publish(new ByteArray(new byte[20]), commitHandler);
//        request = Tests.invoke(clientProtocol, "createRequest");
//        checkLogEntries(request.getLogEntries(), clientId, 0, 3, 2, true);
//
//        for (ByteArray entry : createLogEntries(20, 100))
//            clientProtocol.publish(entry, commitHandler);
//        Tests.invoke(clientProtocol, "removeCommittedEntries", 4);
//
//        request = Tests.invoke(clientProtocol, "createRequest");
//        checkLogEntries(request.getLogEntries(), clientId, 0, 5, 10, true);
//
//        request = Tests.invoke(clientProtocol, "createRequest");
//        checkLogEntries(request.getLogEntries(), clientId, 0, 15, 10, true);
//
//        assertThat(Tests.invoke(clientProtocol, "createRequest"), nullValue());
//    }
//
//    @Test
//    public void testSendRequest() throws Throwable {
//        UUID leaderId = UUID.randomUUID();
//        Tests.set(clientProtocol, "leaderId", leaderId);
//
//        CompletionHandlerMock commitHandler = new CompletionHandlerMock();
//        clientProtocol.publish(new ByteArray(new byte[10]), commitHandler);
//        clientProtocol.publish(new ByteArray(new byte[20]), commitHandler);
//        PublishRequest request = Tests.invoke(clientProtocol, "createRequest");
//
//        messageSender.setFailure(true);
//        messageSender.setError(new RuntimeException("testException"));
//        Tests.invoke(clientProtocol, "sendRequest", request);
//        assertThat(Tests.get(clientProtocol, "leaderId"), nullValue());
//
//        Times.setTest(1000);
//        messageSender.setFailure(false);
//        PublishResponseBuilder response = new PublishResponseBuilder();
//        response.setSource(leaderId);
//        response.setGroupId(initialConfiguration.getGroupId());
//        response.setSource(clientId);
//        response.setAccepted(false);
//        Tests.set(clientProtocol, "leaderId", leaderId);
//        messageSender.setResponse(MessageUtils.write(response.toMessage()));
//        Tests.invoke(clientProtocol, "sendRequest", request);
//        assertThat(Tests.get(clientProtocol, "leaderId"), nullValue());
//
//        ICompartmentTimer heartbeatTimer = Tests.get(clientProtocol, "heartbeatTimer");
//        assertThat(Tests.get(heartbeatTimer, "nextTimerTime"), is(2000L));
//    }
//
//    @Test
//    public void testHandleSucceededAcceptedRequest() throws Throwable {
//        UUID leaderId = UUID.randomUUID();
//        Tests.set(clientProtocol, "leaderId", leaderId);
//
//        CompletionHandlerMock commitHandler = new CompletionHandlerMock();
//        for (ByteArray entry : createLogEntries(10, 100))
//            clientProtocol.publish(entry, commitHandler);
//
//        Times.setTest(1000);
//        PublishResponseBuilder response = new PublishResponseBuilder();
//        response.setSource(leaderId);
//        response.setGroupId(initialConfiguration.getGroupId());
//        response.setSource(clientId);
//        response.setAccepted(true);
//        response.setLastCommittedMessageId(2);
//        response.setConfiguration(createState(10).getConfiguration());
//
//        Tests.invoke(clientProtocol, "handleSucceededResponse", response.toMessage());
//
//        assertThat(Tests.get(clientProtocol, "lastCommittedMessageId"), is(2L));
//        assertThat(Tests.get(clientProtocol, "configuration"), is(response.getConfiguration()));
//
//        ICompartmentTimer heartbeatTimer = Tests.get(clientProtocol, "leaderTimeoutTimer");
//        assertThat(Tests.get(heartbeatTimer, "nextTimerTime"), is(4000L));
//
//        response.setFlags(Enums.of(MessageFlags.OUT_OF_ORDER_RECEIVED));
//        response.setLastReceivedMessageId(3);
//
//        Tests.invoke(clientProtocol, "handleSucceededResponse", response.toMessage());
//
//        assertThat(Tests.get(clientProtocol, "lastSentMessageId"), is(3L));
//
//        response.setLastReceivedMessageId(0);
//
//        messageSender.setFailure(true);
//        messageSender.setError(new RuntimeException("testException"));
//        Tests.invoke(clientProtocol, "handleSucceededResponse", response.toMessage());
//        PublishRequest request = (PublishRequest) MessageUtils.read(messageSender.getRequest());
//        assertThat(request.getSource(), is(clientId));
//        assertThat(request.getGroupId(), is(initialConfiguration.getGroupId()));
//        assertThat(request.getFlags(), is(Enums.of(MessageFlags.RESTART_SESSION)));
//        checkLogEntries(request.getLogEntries(), clientId, 0, 3, 1, false);
//    }
//
//    @Test
//    public void testHandleSucceededRejectedRequest() throws Throwable {
//        Tests.invoke(clientProtocol, "ensureMessageSender");
//        assertThat(Tests.get(clientProtocol, "messageSender") != null, is(true));
//
//        UUID leaderId = UUID.randomUUID();
//        Tests.set(clientProtocol, "leaderId", leaderId);
//
//        CompletionHandlerMock commitHandler = new CompletionHandlerMock();
//        for (ByteArray entry : createLogEntries(10, 100))
//            clientProtocol.publish(entry, commitHandler);
//
//        Times.setTest(1000);
//        PublishResponseBuilder response = new PublishResponseBuilder();
//        response.setSource(leaderId);
//        response.setGroupId(initialConfiguration.getGroupId());
//        response.setSource(clientId);
//        response.setAccepted(false);
//        response.setLastCommittedMessageId(2);
//        response.setConfiguration(createState(10).getConfiguration());
//        response.setLeaderId(leaderId);
//
//        Tests.invoke(clientProtocol, "handleSucceededResponse", response.toMessage());
//        assertThat(Tests.get(clientProtocol, "messageSender") != null, is(true));
//        assertThat(Tests.get(clientProtocol, "leaderId"), is(leaderId));
//
//        response.setLeaderId(null);
//        Tests.invoke(clientProtocol, "handleSucceededResponse", response.toMessage());
//        assertThat(Tests.get(clientProtocol, "leaderId"), nullValue());
//        assertThat(Tests.get(clientProtocol, "messageSender"), nullValue());
//
//        response.setLeaderId(UUID.randomUUID());
//
//        Tests.invoke(clientProtocol, "handleSucceededResponse", response.toMessage());
//        assertThat(Tests.get(clientProtocol, "leaderId"), is(response.getLeaderId()));
//    }
//
//    @Test
//    public void testHandleLeaderTimeout() throws Throwable{
//        UUID leaderId = UUID.randomUUID();
//        Tests.set(clientProtocol, "leaderId", leaderId);
//        Tests.invoke(clientProtocol, "ensureMessageSender");
//
//        Times.setTest(2999);
//        clientProtocol.onTimer(2999);
//
//        Times.setTest(3000);
//        clientProtocol.onTimer(3000);
//        assertThat(Tests.get(clientProtocol, "leaderId"), nullValue());
//    }
//
//    @Test
//    public void testCommit() throws Throwable {
//        CompletionHandlerMock commitHandler1 = new CompletionHandlerMock();
//        CompletionHandlerMock commitHandler2 = new CompletionHandlerMock();
//        CompletionHandlerMock commitHandler3 = new CompletionHandlerMock();
//
//        clientProtocol.publish(new ByteArray(new byte[10]), commitHandler1);
//        clientProtocol.publish(new ByteArray(new byte[10]), commitHandler1);
//        clientProtocol.publish(new ByteArray(new byte[10]), commitHandler2);
//        clientProtocol.publish(new ByteArray(new byte[10]), commitHandler2);
//        SimpleDeque queue = Tests.get(clientProtocol, "queue");
//        assertThat(commitHandler1.getSucceeded(), nullValue());
//        assertThat(commitHandler2.getSucceeded(), nullValue());
//        assertThat(queue.size(), is(4));
//
//        Tests.invoke(clientProtocol, "removeCommittedEntries", 2L);
//
//        assertThat(queue.size(), is(2));
//        assertThat(commitHandler1.getSucceeded() != null, is(true));
//        assertThat(commitHandler2.getSucceeded(), nullValue());
//
//        Tests.invoke(clientProtocol, "removeCommittedEntries", 4L);
//
//        assertThat(commitHandler2.getSucceeded() != null, is(true));
//        assertThat(queue.isEmpty(), is(true));
//
//        clientProtocol.publish(new ByteArray(new byte[10]), commitHandler3);
//        clientProtocol.publish(new ByteArray(new byte[10]), commitHandler3);
//        assertThat(commitHandler3.getSucceeded(), nullValue());
//        assertThat(queue.size(), is(2));
//
//        Tests.invoke(clientProtocol, "removeCommittedEntries", 6L);
//
//        assertThat(commitHandler3.getSucceeded() != null, is(true));
//        assertThat(queue.isEmpty(), is(true));
//    }
//
//    @Test
//    public void testServerConnect() throws Throwable {
//        UUID leaderId = initialConfiguration.getServers().get(0).getId();
//        Tests.set(clientProtocol, "leaderId", leaderId);
//
//        assertThat(Tests.invoke(clientProtocol, "ensureMessageSender") == messageSender, is(true));
//        assertThat(messageSender.getEndpoint(), is(initialConfiguration.getServers().get(0).getEndpoint()));
//
//        Tests.set(clientProtocol, "leaderId", UUID.randomUUID());
//        assertThat(Tests.invoke(clientProtocol, "ensureMessageSender") == messageSender, is(true));
//        assertThat(messageSender.getEndpoint(), is(initialConfiguration.getServers().get(0).getEndpoint()));
//        Tests.invoke(clientProtocol, "onLeaderDisconnected");
//
//        Times.setTest(1000);
//        assertThat(Tests.invoke(clientProtocol, "ensureMessageSender") == messageSender, is(true));
//        boolean found = false;
//        for (ServerConfiguration configuration : initialConfiguration.getServers()) {
//            if (configuration.getEndpoint().equals(messageSender.getEndpoint())) {
//                found = true;
//                break;
//            }
//        }
//
//        assertThat(found, is(true));
//
//        ICompartmentTimer heartbeatTimer = Tests.get(clientProtocol, "heartbeatTimer");
//        assertThat(Tests.get(heartbeatTimer, "nextTimerTime"), is(1000L));
//
//        ICompartmentTimer updateConfigurationTimer = Tests.get(clientProtocol, "updateConfigurationTimer");
//        assertThat(Tests.get(updateConfigurationTimer, "nextTimerTime"), is(3000L));
//
//        ICompartmentTimer leaderTimer = Tests.get(clientProtocol, "leaderTimeoutTimer");
//        assertThat(Tests.get(leaderTimer, "nextTimerTime"), is(4000L));
//    }
//
//    @Test
//    public void testFlowControl() throws Throwable {
//        CompletionHandlerMock commitHandler = new CompletionHandlerMock();
//
//        for (int i = 0; i < 20; i++)
//            clientProtocol.publish(new ByteArray(new byte[10]), commitHandler);
//
//        assertThat(flowController.isFlowLocked(), is(true));
//
//        Tests.invoke(clientProtocol, "removeCommittedEntries", 10L);
//
//        assertThat(commitHandler.getSucceeded() != null, is(true));
//        assertThat(flowController.isFlowLocked(), is(false));
//    }
//
//    @Test
//    public void testHeartbeats() {
//        Times.setTest(1000);
//        clientProtocol.onTimer(1000);
//
//        PublishRequest request = (PublishRequest) MessageUtils.read(messageSender.getRequest());
//        assertThat(request != null, is(true));
//        assertThat(request.getGroupId(), is(initialConfiguration.getGroupId()));
//        assertThat(request.getSource(), is(clientId));
//    }
//
//    @Test
//    public void testRequestConfigurationUpdate() {
//        Times.setTest(1000);
//        clientProtocol.onTimer(1000);
//
//        Times.setTest(3000);
//        clientProtocol.onTimer(3000);
//
//        PublishRequest request = (PublishRequest) MessageUtils.read(messageSender.getRequest());
//        assertThat(request != null, is(true));
//        assertThat(request.getGroupId(), is(initialConfiguration.getGroupId()));
//        assertThat(request.getSource(), is(clientId));
//        assertThat(request.getFlags(), is(Enums.of(MessageFlags.REQUEST_CONFIGURATION)));
//    }
//
//    @Test
//    public void testUpdateConfiguration() throws Throwable {
//        List<ServerConfiguration> list = new ArrayList<>();
//        for (int i = 0; i < 10; i++) {
//            if (i < 5)
//                list.add(new ServerConfiguration(UUID.randomUUID(), "testNewEndpoint-" + i));
//            else
//                list.add(new ServerConfiguration(initialConfiguration.getServers().get(i).getId(), "testModifiedEndpoint-" + i));
//        }
//
//        GroupConfiguration configuration = new GroupConfiguration("test", UUID.randomUUID(), list);
//
//        Tests.invoke(clientProtocol, "updateConfiguration", configuration);
//
//        assertThat(Tests.get(clientProtocol, "initialConfiguration") == initialConfiguration, is(true));
//        assertThat(Tests.get(clientProtocol, "configuration") == configuration, is(true));
//        List<ServerConfiguration> serverConfigurations = Tests.get(clientProtocol, "serverConfigurations");
//        list = new ArrayList<>();
//        for (int i = 0; i < 15; i++) {
//            if (i < 5)
//                list.add(initialConfiguration.getServers().get(i));
//            else if (i < 10)
//                list.add(configuration.getServers().get(i));
//            else
//                list.add(configuration.getServers().get(i - 10));
//        }
//        assertThat(serverConfigurations, is(list));
//
//        assertThat(clientStore.getConfiguration() == configuration, is(true));
//    }
//
//    private List<LogEntry> toList(SimpleDeque queue) throws Throwable {
//        List<Object> infos = queue.toList();
//        List<LogEntry> entries = new ArrayList<>();
//        for (Object info : infos)
//            entries.add(Tests.get(info, "entry"));
//
//        return entries;
//    }
}
