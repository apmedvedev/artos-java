package com.artos.inttests.shards;

import com.artos.inttests.shards.sim.api.ISimDataSpace;
import com.artos.inttests.shards.sim.impl.SimDataSpace;
import com.exametrika.common.utils.Threads;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertTrue;

public class SimDataSpaceTests {
    private ISimDataSpace dataSpace;

    @Before
    public void setUp() {
        dataSpace = new SimDataSpace();
        dataSpace.start();
        Threads.sleep(5000);
    }

    @After
    public void tearDown() {
        dataSpace.stop();
        dataSpace.writeStatistics(new File("/home/apmedvedev/work/work/src/artos/shards/target/stats"));
    }

    @Test
    public void testNormal() {
        dataSpace.getDrivers().enableDrivers(true);
        Threads.sleep(60000);
        assertTrue(dataSpace.getChecks().checkConsistency(10000));
    }
}
