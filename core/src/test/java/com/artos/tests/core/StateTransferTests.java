package com.artos.tests.core;

import com.artos.impl.core.server.state.StateTransferClient;
import com.artos.impl.core.server.state.StateTransferServer;
import com.artos.tests.core.mocks.StateTransferAcquirerMock;
import com.artos.tests.core.mocks.StateTransferApplierMock;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.Classes;
import com.exametrika.common.utils.Files;
import com.exametrika.common.utils.Threads;
import com.exametrika.common.utils.Times;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class StateTransferTests extends AbstractRaftTest {
    private final boolean secured;
    private File workDir;
    private StateTransferServer stateTransferServer;
    private StateTransferClient stateTransferClient;
    private StateTransferAcquirerMock stateAcquirer;
    private StateTransferApplierMock stateApplier;

    @Parameters
    public static Collection<?> parameters() {
        return Arrays.asList(new Object[]{true}, new Object[]{false});
    }

    public StateTransferTests(boolean secured) {
        this.secured = secured;
    }

    @Before
    public void setUp() {
        Times.setTest(0);

        workDir = Files.createTempDirectory("");
        createState(10);
        context = createContext(state.getConfiguration());

        stateAcquirer = new StateTransferAcquirerMock();
        stateApplier = new StateTransferApplierMock();

        String keyStorePassword = "testtest";
        String keyStoreFile = "classpath:" + Classes.getResourcePath(getClass()) + "/keystore.jks";
        stateTransferServer = new StateTransferServer("endpoint", 19000, 20000, "localhost", workDir.getAbsolutePath(), secured,
            keyStoreFile, keyStorePassword, stateAcquirer, context.getCompartment());
        stateTransferServer.start();

        stateTransferClient = new StateTransferClient("endpoint", stateTransferServer.getLocalAddress().getHostName(),
            stateTransferServer.getLocalAddress().getPort(), workDir.getAbsolutePath(), secured,
            keyStoreFile, keyStorePassword, stateApplier);

        Threads.sleep(500);
    }

    @After
    @Override
    public void tearDown() {
        super.tearDown();

        if (stateTransferServer != null)
            stateTransferServer.stop();

        Files.delete(workDir);
    }

    @Test
    public void testStateTransfer() {
        ByteArray snapshot = createBuffer((byte)100, 100000);
        List<ByteArray> logs = new ArrayList<>();
        for (int i = 0; i < 10; i++)
            logs.add(createBuffer((byte)(200 + i), 100000));

        stateAcquirer.setGroupId(context.getGroupId());
        stateAcquirer.setLogIndex(100);
        stateAcquirer.setSnapshot(snapshot);
        stateAcquirer.setLogs(logs);

        stateApplier.setCompressed(true);
        stateApplier.setLogIndex(100);

        stateTransferClient.transfer(context.getGroupId(), 100);

        assertThat(stateApplier.getSnapshot(), is(snapshot));
        assertThat(stateApplier.getLogs().size(), is(10));
        for (int i = 0; i < 10; i++)
            assertThat(stateApplier.getLogs().get(i), is(logs.get(i)));
    }
}
