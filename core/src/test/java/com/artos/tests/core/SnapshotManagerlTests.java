package com.artos.tests.core;

import com.artos.impl.core.server.state.ISnapshotManager;
import com.artos.impl.core.server.state.Snapshot;
import com.artos.impl.core.server.state.SnapshotManagerFactory;
import com.artos.tests.core.mocks.LogStoreMock;
import com.artos.tests.core.mocks.ReplicationProtocolMock;
import com.artos.tests.core.mocks.StateMachineMock;
import com.exametrika.common.tests.Expected;
import com.exametrika.common.tests.Tests;
import com.exametrika.common.utils.Files;
import com.exametrika.common.utils.Times;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

public class SnapshotManagerlTests extends AbstractRaftTest {
    private SnapshotManagerFactory snapshotManagerFactory;
    private ReplicationProtocolMock replicationProtocol;
    private File workDir;
    private StateMachineMock stateMachine;

    @Before
    public void setUp() throws Throwable {
        Times.setTest(0);

        File tempDir = Files.createTempDirectory("");
        createState(10);
        context = createContext(state.getConfiguration());

        stateMachine = (StateMachineMock) context.getStateMachine();

        Tests.set(context.getServerChannelConfiguration(), "workDir", tempDir.getAbsolutePath());
        workDir = new File(tempDir, "snapshots");
        Tests.set(context.getServerChannelConfiguration(), "stateTransferUseCompression", true);
        replicationProtocol = new ReplicationProtocolMock();
        snapshotManagerFactory = new SnapshotManagerFactory(context);
        snapshotManagerFactory.setReplicationProtocol(replicationProtocol);
        snapshotManagerFactory.start();
    }

    @After
    @Override
    public void tearDown() {
        super.tearDown();

        Files.delete(workDir.getParentFile());
    }

    @Test
    public void testCreateSnapshotSucceeded() throws Throwable {
        Times.setTest(300);
        stateMachine.setData(createBuffer((byte)10, 1000000));
        stateMachine.getTransaction().setLastCommitIndex(10);

        ISnapshotManager snapshotManager = snapshotManagerFactory.create();
        new Expected(IllegalStateException.class, (Runnable) () -> snapshotManagerFactory.create());

        assertThat(snapshotManager.getSnapshot(), nullValue());
        Snapshot snapshot = snapshotManager.createSnapshot();
        assertThat(snapshot.getLogIndex(), is(10L));
        assertThat(snapshot.getCreationTime(), is(300L));
        assertThat(snapshot.getFile().getParentFile(), is(workDir));

        assertThat(replicationProtocol.isCommitsLocked(), is(true));
        assertThat(replicationProtocol.isCommitsUnlocked(), is(true));
        assertThat(snapshotManager.getSnapshot() == snapshot, is(true));

        snapshotManager.close();

        ISnapshotManager snapshotManager1 = snapshotManagerFactory.create();
        Snapshot loadedSnapshot = snapshotManager1.getSnapshot();
        assertThat(loadedSnapshot.getLogIndex(), is(snapshot.getLogIndex()));
        assertThat(loadedSnapshot.getFile(), is(snapshot.getFile()));
        assertThat(loadedSnapshot.getCreationTime(), is(snapshot.getCreationTime()));

        snapshotManagerFactory.stop();
        new Expected(IllegalStateException.class, (Runnable) () -> snapshotManager1.getSnapshot());

        snapshotManagerFactory = new SnapshotManagerFactory(context);
        snapshotManagerFactory.setReplicationProtocol(replicationProtocol);
        snapshotManagerFactory.start();

        ISnapshotManager snapshotManager2 = snapshotManagerFactory.create();
        loadedSnapshot = snapshotManager2.getSnapshot();
        assertThat(loadedSnapshot.getLogIndex(), is(snapshot.getLogIndex()));
        assertThat(loadedSnapshot.getFile(), is(snapshot.getFile()));
        assertThat(loadedSnapshot.getCreationTime(), is(snapshot.getCreationTime()));

        snapshotManagerFactory.uninstall();

        assertThat(workDir.listFiles().length, is(0));
    }

    @Test
    public void testCreateSnapshotFailed() throws Throwable {
        stateMachine.setData(createBuffer((byte) 10, 1000000));
        stateMachine.getTransaction().setLastCommitIndex(10);
        stateMachine.setError(new RuntimeException("testException"));

        ISnapshotManager snapshotManager = snapshotManagerFactory.create();
        new Expected(RuntimeException.class, (Runnable) () -> snapshotManager.createSnapshot());

        assertThat(replicationProtocol.isCommitsLocked(), is(true));
        assertThat(replicationProtocol.isCommitsUnlocked(), is(true));
        assertThat(snapshotManager.getSnapshot(), nullValue());
    }

    @Test
    public void testRemoveSnapshot() throws Throwable {
        stateMachine.setData(createBuffer((byte) 10, 1000000));
        stateMachine.getTransaction().setLastCommitIndex(10);

        ISnapshotManager snapshotManager = snapshotManagerFactory.create();
        new Expected(IllegalStateException.class, (Runnable) () -> snapshotManagerFactory.create());

        assertThat(snapshotManager.getSnapshot(), nullValue());
        Snapshot snapshot = snapshotManager.createSnapshot();
        assertThat(snapshot != null, is(true));

        snapshotManager.removeSnapshot();
        assertThat(snapshotManager.getSnapshot(), nullValue());

        assertThat(workDir.listFiles().length, is(0));
    }

    @Test
    public void testRemoveOutdatedSnapshot() throws Throwable {
        stateMachine.setData(createBuffer((byte) 10, 1000000));
        stateMachine.getTransaction().setLastCommitIndex(10);

        ISnapshotManager snapshotManager = snapshotManagerFactory.create();
        new Expected(IllegalStateException.class, (Runnable) () -> snapshotManagerFactory.create());

        assertThat(snapshotManager.getSnapshot(), nullValue());
        Snapshot snapshot = snapshotManager.createSnapshot();
        assertThat(snapshot != null, is(true));

        ((LogStoreMock)context.getLogStore()).setStartIndex(11);

        assertThat(snapshotManager.getSnapshot(), nullValue());

        assertThat(workDir.listFiles().length, is(0));
    }
}
