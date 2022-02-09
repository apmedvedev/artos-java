package com.artos.tests.core;

import com.artos.api.core.server.conf.ServerChannelFactoryConfigurationBuilder;
import com.artos.api.core.server.conf.ServerConfiguration;
import com.artos.impl.core.message.AppendEntriesRequest;
import com.artos.impl.core.message.ServerResponse;
import com.artos.impl.core.server.impl.Context;
import com.artos.impl.core.server.impl.ServerPeer;
import com.artos.impl.core.server.impl.State;
import com.artos.impl.core.server.utils.MessageUtils;
import com.artos.tests.core.mocks.MessageSenderFactoryMock;
import com.artos.tests.core.mocks.MessageSenderMock;
import com.exametrika.common.tests.Expected;
import com.exametrika.common.tests.Tests;
import com.exametrika.common.utils.SyncCompletionHandler;
import com.exametrika.common.utils.Times;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ServerPeerTest extends AbstractRaftTest {
    @Before
    public void setUp() throws Throwable {
        Times.setTest(0);

        createState(10);
        context = createContext(state.getConfiguration());
    }

    @Test
    public void testServerPeer() throws Throwable {
        Times.setTest(1000);
        State state = createState(1);
        Context context = createContext(state.getConfiguration());

        ServerChannelFactoryConfigurationBuilder builder = new ServerChannelFactoryConfigurationBuilder();
        builder.setHeartbeatPeriod(1000).setMinElectionTimeout(5000).setSendFailureBackoff(1000);
        Tests.set(context, "serverChannelFactoryConfiguration", builder.toConfiguration());

        ServerConfiguration configuration = state.getConfiguration().getServers().get(0);
        ServerPeer peer = new ServerPeer(configuration, context, null);
        assertThat(peer.canHeartbeat(1500), is(false));
        assertThat(peer.getLastResponseTime(), is(1000L));

        Times.setTest(2000);
        peer.start();
        assertThat(peer.canHeartbeat(2500), is(false));
        assertThat(peer.canHeartbeat(3000), is(true));
        assertThat(peer.getLastResponseTime(), is(2000L));

        AppendEntriesRequest request = createAppendEntriesRequest().toMessage();
        SyncCompletionHandler completionHandler = new SyncCompletionHandler();
        MessageSenderFactoryMock messageSenderFactory = (MessageSenderFactoryMock)context.getMessageSenderFactory();
        MessageSenderMock messageSender = messageSenderFactory.getSender();

        Times.setTest(3000);
        ServerResponse response = createAppendEntriesResponse();
        messageSender.setResponse(MessageUtils.write(response));
        peer.send(request/*, completionHandler*/);
        completionHandler.await();
        assertThat(peer.getLastResponseTime(), is(3000L));

        Times.setTest(4000);
        messageSender.setFailure(true);
        messageSender.setError(new RuntimeException());
        SyncCompletionHandler completionHandler2 = new SyncCompletionHandler();
        peer.send(request/*, completionHandler2*/);
        new Expected(RuntimeException.class, (Runnable) () -> { completionHandler2.await(); });
        assertThat(peer.getLastResponseTime(), is(4000L));

        assertThat(peer.canHeartbeat(5500), is(false));
        assertThat(peer.canHeartbeat(6000), is(true));

        Times.setTest(6000);
        messageSender.setFailure(false);
        completionHandler = new SyncCompletionHandler();
        peer.send(request/*8, completionHandler*/);
        completionHandler.await();

        assertThat(peer.canHeartbeat(7000), is(true));
    }
}
