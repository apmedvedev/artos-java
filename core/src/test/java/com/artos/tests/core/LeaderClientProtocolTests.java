package com.artos.tests.core;

import com.artos.api.core.server.conf.ServerChannelFactoryConfigurationBuilder;
import com.artos.impl.core.server.client.LeaderClientProtocol;
import com.artos.impl.core.server.impl.ServerRole;
import com.artos.tests.core.mocks.CompletionHandlerMock;
import com.artos.tests.core.mocks.FlowControllerMock;
import com.artos.tests.core.mocks.ReplicationProtocolMock;
import com.exametrika.common.tests.Expected;
import com.exametrika.common.tests.Tests;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.SimpleDeque;
import com.exametrika.common.utils.Threads;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

public class LeaderClientProtocolTests extends AbstractRaftTest {
    private LeaderClientProtocol leaderClientProtocol;
    private ReplicationProtocolMock replicationProtocol;
    private FlowControllerMock flowController;

    @Before
    public void setUp() throws Throwable {
        createState(10);
        context = createContext(state.getConfiguration());

        ServerChannelFactoryConfigurationBuilder builder = new ServerChannelFactoryConfigurationBuilder();
        builder.setMaxPublishingLogEntryCount(10).setHeartbeatPeriod(1000).setMinStateTransferGapLogEntryCount(30)
            .setClientSessionTimeout(10000);
        Tests.set(context, "serverChannelFactoryConfiguration", builder.toConfiguration());

        replicationProtocol = new ReplicationProtocolMock();
        flowController = new FlowControllerMock();

        leaderClientProtocol = new LeaderClientProtocol(state, 100, 20, 10, 100);
        leaderClientProtocol.setContext(context);
        leaderClientProtocol.setReplicationProtocol(replicationProtocol);
        leaderClientProtocol.setFlowController(flowController);
        context.getCompartment().addTimerProcessor(leaderClientProtocol);
    }

    @Test
    public void testAppendEntries() throws Throwable {
        CompletionHandlerMock commitHandler = new CompletionHandlerMock();
        new Expected(IllegalArgumentException.class, (Runnable) () -> leaderClientProtocol.publish(
            new ByteArray(new byte[10]), new CompletionHandlerMock()));

        leaderClientProtocol.publish(new ByteArray(new byte[10]), commitHandler);
        Threads.sleep(500);
        assertThat(commitHandler.getFailed() != null, is(true));
        commitHandler.reset();

        state.setRole(ServerRole.LEADER);
        SimpleDeque queue = Tests.get(leaderClientProtocol, "queue");

        leaderClientProtocol.publish(new ByteArray(new byte[10]), commitHandler);
        leaderClientProtocol.publish(new ByteArray(new byte[20]), commitHandler);
        Threads.sleep(500);
        checkLogEntries(replicationProtocol.getLogEntries(), null, 0, 1, 2, true);
        assertThat(commitHandler.getSucceeded(), nullValue());
        assertThat(commitHandler.getFailed(), nullValue());
        assertThat(queue.size(), is(2));

        replicationProtocol.setLastCommittedMessageId(4);
        leaderClientProtocol.publish(new ByteArray(new byte[10]), commitHandler);
        leaderClientProtocol.publish(new ByteArray(new byte[20]), commitHandler);
        Threads.sleep(500);
        checkLogEntries(replicationProtocol.getLogEntries(), null, 0, 1, 4, true);
        assertThat(commitHandler.getSucceeded() != null, is(true));
        commitHandler.reset();

        replicationProtocol.setLastCommittedMessageId(6);
        leaderClientProtocol.publish(new ByteArray(new byte[10]), commitHandler);
        leaderClientProtocol.publish(new ByteArray(new byte[20]), commitHandler);
        Threads.sleep(500);
        checkLogEntries(replicationProtocol.getLogEntries(), null, 0, 1, 6, true);
        assertThat(commitHandler.getSucceeded() != null, is(true));

        assertThat(queue.isEmpty(), is(true));
    }

    @Test
    public void testCommit() throws Throwable {
        CompletionHandlerMock commitHandler1 = new CompletionHandlerMock();
        CompletionHandlerMock commitHandler2 = new CompletionHandlerMock();
        CompletionHandlerMock commitHandler3 = new CompletionHandlerMock();
        state.setRole(ServerRole.LEADER);

        leaderClientProtocol.publish(new ByteArray(new byte[10]), commitHandler1);
        leaderClientProtocol.publish(new ByteArray(new byte[10]), commitHandler1);
        leaderClientProtocol.publish(new ByteArray(new byte[10]), commitHandler2);
        leaderClientProtocol.publish(new ByteArray(new byte[10]), commitHandler2);
        Threads.sleep(500);
        checkLogEntries(replicationProtocol.getLogEntries(), null, 0, 1, 4, true);
        SimpleDeque queue = Tests.get(leaderClientProtocol, "queue");
        assertThat(commitHandler1.getSucceeded(), nullValue());
        assertThat(commitHandler2.getSucceeded(), nullValue());
        assertThat(queue.size(), is(4));

        replicationProtocol.setLastCommittedMessageId(2);

        Threads.sleep(500);
        assertThat(queue.size(), is(2));
        assertThat(commitHandler1.getSucceeded() != null, is(true));
        assertThat(commitHandler2.getSucceeded(), nullValue());

        replicationProtocol.setLastCommittedMessageId(4);
        Threads.sleep(500);

        assertThat(commitHandler2.getSucceeded() != null, is(true));
        assertThat(queue.isEmpty(), is(true));

        leaderClientProtocol.publish(new ByteArray(new byte[10]), commitHandler3);
        leaderClientProtocol.publish(new ByteArray(new byte[10]), commitHandler3);
        Threads.sleep(500);
        checkLogEntries(replicationProtocol.getLogEntries(), null, 0, 1, 6, true);
        assertThat(commitHandler3.getSucceeded(), nullValue());
        assertThat(queue.size(), is(2));

        replicationProtocol.setLastCommittedMessageId(6);
        Threads.sleep(500);

        assertThat(commitHandler3.getSucceeded() != null, is(true));
        assertThat(queue.isEmpty(), is(true));
    }

    @Test
    public void testFlowControl() {
        CompletionHandlerMock commitHandler = new CompletionHandlerMock();
        state.setRole(ServerRole.LEADER);

        for (int i = 0; i < 20; i++)
            leaderClientProtocol.publish(new ByteArray(new byte[10]), commitHandler);

        Threads.sleep(500);
        assertThat(flowController.isFlowLocked(), is(true));

        replicationProtocol.setLastCommittedMessageId(10);
        Threads.sleep(500);

        assertThat(commitHandler.getSucceeded() != null, is(true));
        assertThat(flowController.isFlowLocked(), is(false));
    }
}
