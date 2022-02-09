package com.artos.tests.core;

import com.artos.api.core.server.conf.ServerConfiguration;
import com.artos.impl.core.message.JoinGroupRequestBuilder;
import com.artos.impl.core.message.JoinGroupResponseBuilder;
import com.artos.impl.core.server.client.FollowerMessageSender;
import com.artos.impl.core.server.utils.MessageUtils;
import com.artos.tests.core.mocks.CompartmentMock;
import com.artos.tests.core.mocks.CompletionHandlerMock;
import com.artos.tests.core.mocks.MessageSenderMock;
import com.exametrika.common.tests.Tests;
import com.exametrika.common.utils.Times;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

public class FollowerMessageSenderTests extends AbstractRaftTest {
    private CompartmentMock compartment;
    private FollowerMessageSender followerMessageSender;
    private MessageSenderMock messageSender;

    @Before
    public void setUp() throws Throwable {
        createState(10);
        context = createContext(state.getConfiguration());
        messageSender = (MessageSenderMock) context.getMessageSenderFactory().createSender("testEndpoint", null);
        compartment = new CompartmentMock();
        Tests.set(context, "compartment", compartment);
        followerMessageSender = new FollowerMessageSender(context, state, null);
    }

    @Test
    public void testSendRequest() throws Throwable {
        UUID leaderId = state.getConfiguration().getServers().get(1).getId();
        Tests.set(followerMessageSender, "leaderId", leaderId);

        JoinGroupRequestBuilder request = new JoinGroupRequestBuilder();
        request.setGroupId(context.getGroupId());
        request.setSource(leaderId);
        request.setConfiguration(new ServerConfiguration(UUID.randomUUID(), "testEnpoint"));

        CompletionHandlerMock completionHandler = new CompletionHandlerMock();
        messageSender.setFailure(true);
        messageSender.setError(new RuntimeException("testException"));
        Tests.invoke(followerMessageSender, "sendRequest", request.toMessage(), completionHandler);
        assertThat(Tests.get(followerMessageSender, "leaderId"), nullValue());
        assertThat(completionHandler.getFailed() != null, is(true));
        completionHandler.reset();

        messageSender.setFailure(false);
        JoinGroupResponseBuilder response = new JoinGroupResponseBuilder();
        response.setGroupId(context.getGroupId());
        response.setSource(leaderId);
        response.setAccepted(false);
        Tests.set(followerMessageSender, "leaderId", leaderId);
        messageSender.setResponse(MessageUtils.write(response.toMessage()));
        Tests.invoke(followerMessageSender, "sendRequest", request.toMessage(), completionHandler);
        assertThat(Tests.get(followerMessageSender, "messageSender") != null, is(true));
        assertThat(Tests.get(followerMessageSender, "leaderId"), is(leaderId));
        assertThat(completionHandler.getFailed() != null, is(true));
        completionHandler.reset();

        response.setSource(UUID.randomUUID());
        messageSender.setResponse(MessageUtils.write(response.toMessage()));
        Tests.invoke(followerMessageSender, "sendRequest", request.toMessage(), completionHandler);
        assertThat(Tests.get(followerMessageSender, "messageSender") == null, is(true));
        assertThat(Tests.get(followerMessageSender, "leaderId"), is(response.getSource()));
        assertThat(completionHandler.getFailed() != null, is(true));

        completionHandler.reset();

        response.setSource(leaderId);
        response.setAccepted(true);
        messageSender.setResponse(MessageUtils.write(response.toMessage()));
        Tests.set(followerMessageSender, "leaderId", leaderId);
        Tests.invoke(followerMessageSender, "sendRequest", request.toMessage(), completionHandler);
        assertThat(Tests.get(followerMessageSender, "messageSender") != null, is(true));
        assertThat(Tests.get(followerMessageSender, "leaderId"), is(leaderId));
        assertThat(completionHandler.getSucceeded() != null, is(true));
    }

    @Test
    public void testServerConnect() throws Throwable {
        UUID leaderId = state.getConfiguration().getServers().get(1).getId();
        Tests.set(followerMessageSender, "leaderId", leaderId);

        assertThat(Tests.invoke(followerMessageSender, "ensureMessageSender", new Object[]{ null }) == messageSender, is(true));
        assertThat(messageSender.getEndpoint(), is(state.getConfiguration().getServers().get(1).getEndpoint()));

        Tests.set(followerMessageSender, "leaderId", UUID.randomUUID());
        assertThat(Tests.invoke(followerMessageSender, "ensureMessageSender", new Object[]{ null }) == messageSender, is(true));
        assertThat(messageSender.getEndpoint(), is(state.getConfiguration().getServers().get(1).getEndpoint()));
        Tests.invoke(followerMessageSender, "onLeaderDisconnected");

        Times.setTest(1000);
        assertThat(Tests.invoke(followerMessageSender, "ensureMessageSender", new Object[]{ null }) == messageSender, is(true));
        boolean found = false;
        for (ServerConfiguration configuration : state.getConfiguration().getServers()) {
            if (configuration.getEndpoint().equals(messageSender.getEndpoint())) {
                found = true;
                break;
            }
        }

        assertThat(found, is(true));
    }
}
