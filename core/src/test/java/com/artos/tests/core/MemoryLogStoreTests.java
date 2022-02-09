package com.artos.tests.core;

import com.artos.impl.core.server.log.MemoryLogStore;
import com.artos.spi.core.LogEntry;
import com.artos.spi.core.LogValueType;
import com.exametrika.common.io.impl.ByteInputStream;
import com.exametrika.common.io.impl.ByteOutputStream;
import com.exametrika.common.tests.Tests;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.Times;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class MemoryLogStoreTests extends AbstractRaftTest {
    private MemoryLogStore logStore;

    @Before
    public void setUp() throws Throwable {
        Times.setTest(0);
        logStore = new MemoryLogStore(0);
        createState(10);
        context = createContext(state.getConfiguration());
        Tests.set(context, "logStore", logStore);
    }

    @After
    @Override
    public void tearDown() {
        Times.clearTest();
    }

    @Test
    public void testLogStore() {
        UUID clientId = UUID.randomUUID();

        checkLogEntries(null, 0, 1, 1, 0);

        addLogEntries(clientId, 1, 10);

        checkLogEntries(clientId, 1, 1, 11, 0);

        Times.setTest(1000);
        logStore.commit(5);

        checkLogEntries(clientId, 1, 5, 11, 5);

        Times.setTest(2000);
        logStore.lockCompaction();
        logStore.commit(6);
        checkLogEntries(clientId, 1, 5, 11, 6);
        logStore.unlockCompaction();
        checkLogEntries(clientId, 1, 6, 11, 6);

        addLogEntries(clientId, 1, 10);

        checkLogEntries(clientId, 1, 6, 21, 6);

        logStore.setAt(10, new LogEntry(1, null, LogValueType.APPLICATION, clientId, 10));
        checkLogEntries(clientId, 1, 6, 11, 6);

        logStore.clear(10);
        checkLogEntries(null, 0, 10, 10, 10);
    }

    @Test
    public void testReadWrite() {
        UUID clientId = UUID.randomUUID();
        for (int i = 1; i < 100; i++)
            context.getLogStore().append(new LogEntry(1, new ByteArray(new byte[1]), LogValueType.APPLICATION,
                clientId, context.getLogStore().getEndIndex()));

        Times.setTest(1000);
        logStore.commit(10);

        ByteOutputStream out = new ByteOutputStream();
        assertThat(logStore.read(50, out, 20), is(70L));

        ByteInputStream in = new ByteInputStream(out.getBuffer(), 0, out.getLength());
        logStore.write(in);

        checkLogEntries(clientId, 1, 10, 70, 10);
    }

    private void checkLogEntries(UUID clientId, long term, long start, long end, long commit) {
        assertThat(logStore.getStartIndex(), is(start));
        assertThat(logStore.getEndIndex(), is(end));
        assertThat(logStore.getCommitIndex(), is(commit));

        if (start != end)
            checkLogEntries(Collections.singletonList(logStore.getLast()), clientId, term, end - 1, 1, false);
        else
            checkLogEntries(Collections.singletonList(logStore.getLast()), clientId, term, 0, 1, false);

        checkLogEntries(logStore.get(start, end), clientId, term, start, (int) (end - start), false);

        for (long i = start; i < end; i++)
            checkLogEntries(Collections.singletonList(logStore.getAt(i)), clientId, term, i, 1, false);
    }
}
