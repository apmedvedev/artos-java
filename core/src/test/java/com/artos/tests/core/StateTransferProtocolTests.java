package com.artos.tests.core;

import com.artos.impl.core.server.impl.Context;
import com.artos.impl.core.server.impl.State;
import com.artos.impl.core.server.state.StateTransferProtocol;
import com.artos.tests.core.mocks.LogStoreMock;
import com.artos.tests.core.mocks.MembershipProtocolMock;
import com.artos.tests.core.mocks.ReplicationProtocolMock;
import com.artos.tests.core.mocks.StateMachineMock;
import com.exametrika.common.l10n.SystemException;
import com.exametrika.common.tests.Expected;
import com.exametrika.common.tests.Tests;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.Files;
import com.exametrika.common.utils.SyncCompletionHandler;
import com.exametrika.common.utils.Times;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class StateTransferProtocolTests extends AbstractRaftTest {
    private File workDir1;
    private File workDir2;
    private StateTransferProtocol stateTransferServer;
    private StateTransferProtocol stateTransferClient;
    private MembershipProtocolMock membershipProtocol;
    private ReplicationProtocolMock replicationProtocol;
    private State state1;
    private State state2;
    private Context context1;
    private Context context2;

    @Before
    public void setUp() throws Throwable {
        Times.setTest(0);

        workDir1 = Files.createTempDirectory("");
        workDir2 = Files.createTempDirectory("");
        state1 = createState(10);
        context1 = createContext(state1.getConfiguration());

        state2 = createState(10);
        context2 = createContext(state2.getConfiguration());
        Tests.set(context2, "groupId", context1.getGroupId());

        Tests.set(context1.getServerChannelConfiguration(), "workDir", workDir1.getAbsolutePath());
        Tests.set(context2.getServerChannelConfiguration(), "workDir", workDir2.getAbsolutePath());

        replicationProtocol = new ReplicationProtocolMock();
        membershipProtocol = new MembershipProtocolMock();

        stateTransferServer = new StateTransferProtocol(context1, state1);
        stateTransferServer.setReplicationProtocol(replicationProtocol);

        stateTransferClient = new StateTransferProtocol(context2, state2);
        stateTransferClient.setMembershipProtocol(membershipProtocol);
    }

    @After
    @Override
    public void tearDown() {
        super.tearDown();

        if (stateTransferClient != null)
            stateTransferClient.stop();

        if (stateTransferServer != null)
            stateTransferServer.stop();

        Files.delete(workDir1);
        Files.delete(workDir2);
    }

    @Test
    public void testStateTransfer() throws Throwable {
        StateMachineMock stateMachine1 = (StateMachineMock) context1.getStateMachine();
        stateMachine1.getTransaction().setLastCommitIndex(500);
        ByteArray buffer = createBuffer((byte)300, 100000);
        stateMachine1.setData(buffer);

        UUID clientId = UUID.randomUUID();
        context = context1;
        addLogEntries(clientId, 1, 1000);
        LogStoreMock logStore1 = (LogStoreMock) context1.getLogStore();
        logStore1.setStartIndex(500);
        logStore1.setCommitIndex(500);
        stateTransferServer.onLeader();

        SyncCompletionHandler completionHandler = new SyncCompletionHandler();
        stateTransferClient.requestStateTransfer(stateTransferServer.getHost(), stateTransferServer.getPort(), 100, completionHandler);
        SyncCompletionHandler completionHandler2 = new SyncCompletionHandler();
        stateTransferClient.requestStateTransfer(stateTransferServer.getHost(), stateTransferServer.getPort(), 100, completionHandler2);
        new Expected(SystemException.class, (Runnable)() -> completionHandler2.await(1000));

        completionHandler.await(1000);
        assertThat(logStore1.isCompactionLocked(), is(true));
        assertThat(logStore1.isCompactionUnlocked(), is(true));
        assertThat(replicationProtocol.isCommitsLocked(), is(true));
        assertThat(replicationProtocol.isCommitsUnlocked(), is(true));

        StateMachineMock stateMachine2 = (StateMachineMock) context2.getStateMachine();
        assertThat(stateMachine2.getData(), is(buffer));
        LogStoreMock logStore2 = (LogStoreMock) context2.getLogStore();
        assertThat(logStore2.getStartIndex(), is(500L));
        assertThat(logStore2.getCommitIndex(), is(500L));
        checkLogEntries(logStore2.getEntries(), clientId, 1, 1, 1000, true);

        stateTransferServer.onFollower();
        new Expected(IllegalStateException.class, (Runnable)() -> stateTransferServer.getHost());

        assertThat(membershipProtocol.getNewConfiguration(), is(state1.getConfiguration()));
    }

}
