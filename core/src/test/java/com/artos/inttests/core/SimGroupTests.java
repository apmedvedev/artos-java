package com.artos.inttests.core;

import com.artos.impl.core.server.election.ElectionProtocol;
import com.artos.impl.core.server.replication.ReplicationProtocol;
import com.artos.inttests.core.sim.api.ISimGroup;
import com.artos.inttests.core.sim.impl.TestInterceptor;
import com.artos.inttests.core.sim.intercept.SimGroupInterceptors;
import com.exametrika.common.utils.Pair;
import com.exametrika.common.utils.Threads;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

import static org.junit.Assert.assertTrue;

public class SimGroupTests {
    private ISimGroup group;

    @Before
    public void setUp() {
        group = new SimGroupInterceptors().createGroup(Arrays.asList(
            new Pair<>(ReplicationProtocol.class.getName() + ".createAppendEntriesRequest*", TestInterceptor.class),
            new Pair<>(ReplicationProtocol.class.getName() + ".requestAppendEntries*", TestInterceptor.class),
            new Pair<>(ElectionProtocol.class.getName() + ".restartElectionTimer*", TestInterceptor.class),
            new Pair<>(ElectionProtocol.class.getName() + ".becomeCandidate*", TestInterceptor.class),
            new Pair<>(ElectionProtocol.class.getName() + ".becomeFollower*", TestInterceptor.class)
        ));
        group.start();
        Threads.sleep(5000);
    }

    @After
    public void tearDown() {
        group.stop();
        group.writeStatistics(new File("/home/apmedvedev/work/work/src/artos/core/target/stats"));
    }

    @Test
    public void testNormal() {
        group.getDrivers().enableDrivers(true);
        Threads.sleep(60000);
        assertTrue(group.getChecks().checkConsistency(10000));
    }

    @Test
    public void testJoinServer() {
        group.getDrivers().enableDrivers(true);
        Threads.sleep(60000);
        group.getServers().joinServer();
        Threads.sleep(60000);
        assertTrue(group.getChecks().checkConsistency(10000));
    }

    @Test
    public void testLeaveLeader() {
        group.getDrivers().enableDrivers(true);
        Threads.sleep(60000);
        group.getServers().leaveServer(true);
        Threads.sleep(60000);
        assertTrue(group.getChecks().checkConsistency(10000));
    }

    @Test
    public void testLeaveFollower() {
        group.getDrivers().enableDrivers(true);
        Threads.sleep(60000);
        group.getServers().leaveServer(false);
        Threads.sleep(60000);
        assertTrue(group.getChecks().checkConsistency(10000));
    }

    @Test
    public void testGracefulStopLeader() {
        group.getDrivers().enableDrivers(true);
        Threads.sleep(60000);
        group.getServers().gracefullyStopServer(true);
        Threads.sleep(60000);
        assertTrue(group.getChecks().checkConsistency(10000));
    }

    @Test
    public void testGracefulStopFollower() {
        group.getDrivers().enableDrivers(true);
        Threads.sleep(60000);
        group.getServers().gracefullyStopServer(false);
        Threads.sleep(60000);
        assertTrue(group.getChecks().checkConsistency(10000));
    }

    @Test
    public void testFailLeader() {
        group.getDrivers().enableDrivers(true);
        Threads.sleep(60000);
        group.getServers().failServer(true);
        Threads.sleep(60000);
        assertTrue(group.getChecks().checkConsistency(10000));
    }

    @Test
    public void testFailFollower() {
        group.getDrivers().enableDrivers(true);
        Threads.sleep(60000);
        group.getServers().failServer(false);
        Threads.sleep(60000);
        assertTrue(group.getChecks().checkConsistency(10000));
    }

    @Test
    public void testLeaveRestoreLeader() {
        group.getDrivers().enableDrivers(true);
        Threads.sleep(60000);
        group.getServers().leaveServer(true);
        Threads.sleep(60000);
        group.getServers().restoreServer();
        Threads.sleep(60000);
        assertTrue(group.getChecks().checkConsistency(10000));
    }

    @Test
    public void testLeaveRestoreFollower() {
        group.getDrivers().enableDrivers(true);
        Threads.sleep(60000);
        group.getServers().leaveServer(false);
        Threads.sleep(60000);
        group.getServers().restoreServer();
        Threads.sleep(60000);
        assertTrue(group.getChecks().checkConsistency(10000));
    }

    @Test
    public void testFailRestoreLeader() {
        group.getDrivers().enableDrivers(true);
        Threads.sleep(60000);
        group.getServers().failServer(true);
        Threads.sleep(60000);
        group.getServers().restoreServer();
        Threads.sleep(60000);
        assertTrue(group.getChecks().checkConsistency(10000));
    }

    @Test
    public void testFailRestoreFollower() {
        group.getDrivers().enableDrivers(true);
        Threads.sleep(60000);
        group.getServers().failServer(false);
        Threads.sleep(60000);
        group.getServers().restoreServer();
        Threads.sleep(60000);
        assertTrue(group.getChecks().checkConsistency(10000));
    }

    @Test
    public void testShortLinkFailureLeader() {
        group.getDrivers().enableDrivers(true);
        Threads.sleep(60000);
        group.getServers().disableLink(true);
        Threads.sleep(5000);
        group.getLinks().enableAllLinks();
        Threads.sleep(60000);
        assertTrue(group.getChecks().checkConsistency(10000));
    }

    @Test
    public void testLongLinkFailureLeader() {
        group.getDrivers().enableDrivers(true);
        Threads.sleep(60000);
        group.getServers().disableLink(true);
        Threads.sleep(60000);
        group.getLinks().enableAllLinks();
        Threads.sleep(60000);
        assertTrue(group.getChecks().checkConsistency(10000));
    }

    @Test
    public void testShortLinkFailureFollower() {
        group.getDrivers().enableDrivers(true);
        Threads.sleep(60000);
        group.getServers().disableLink(false);
        Threads.sleep(5000);
        group.getLinks().enableAllLinks();
        Threads.sleep(60000);
        assertTrue(group.getChecks().checkConsistency(10000));
    }

    @Test
    public void testLongLinkFailureFollower() {
        group.getDrivers().enableDrivers(true);
        Threads.sleep(60000);
        group.getServers().disableLink(false);
        Threads.sleep(60000);
        group.getLinks().enableAllLinks();
        Threads.sleep(60000);
        assertTrue(group.getChecks().checkConsistency(10000));
    }

    @Test
    public void testMajorityWithLeaderFailureRestore() {
        group.getDrivers().enableDrivers(true);
        Threads.sleep(60000);
        group.getServers().failServer(true);
        group.getServers().failServer(false);
        group.getServers().failServer(false);
        Threads.sleep(10000);
        group.getServers().restoreServer();
        Threads.sleep(60000);
        assertTrue(group.getChecks().checkConsistency(10000));
    }

    @Test
    public void testMajorityWithFollowerFailureRestore() {
        group.getDrivers().enableDrivers(true);
        Threads.sleep(60000);
        group.getServers().failServer(false);
        group.getServers().failServer(false);
        group.getServers().failServer(false);
        Threads.sleep(10000);
        group.getServers().restoreServer();
        Threads.sleep(60000);
        assertTrue(group.getChecks().checkConsistency(10000));
    }

    @Test
    public void testMajorityWithLeaderFailure() {
        group.getDrivers().enableDrivers(true);
        Threads.sleep(60000);
        group.getServers().failServer(true);
        group.getServers().failServer(false);
        group.getServers().failServer(false);
        Threads.sleep(10000);
        group.getServers().failServer(false);
        group.getServers().failServer(false);
        group.getServers().restoreServer();
        group.getServers().restoreServer();
        Threads.sleep(60000);
        assertTrue(group.getChecks().checkConsistency(10000));
    }

    @Test
    public void testMajorityWithoutLeaderFailure() {
        group.getDrivers().enableDrivers(true);
        Threads.sleep(60000);
        group.getServers().failServer(false);
        group.getServers().failServer(false);
        group.getServers().failServer(false);
        Threads.sleep(10000);
        group.getServers().failServer(true);
        group.getServers().failServer(false);
        group.getServers().restoreServer();
        group.getServers().restoreServer();
        Threads.sleep(60000);
        assertTrue(group.getChecks().checkConsistency(10000));
    }
}
