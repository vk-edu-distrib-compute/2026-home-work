package company.vk.edu.distrib.compute.ronshinvsevolod.consensus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsensusTest {

    private static final int CLUSTER_SIZE = 5;
    private Cluster cluster;

    @BeforeEach
    void setUp() {
        cluster = new Cluster(CLUSTER_SIZE);
        cluster.start();
    }

    @AfterEach
    void tearDown() {
        cluster.stop();
    }

    private int waitForLeader() throws InterruptedException {
        Thread.sleep(Node.getElectionSettleMs());
        return cluster.getLeaderId();
    }

    @Test
    void leaderElectedOnStart() throws InterruptedException {
        assertEquals(CLUSTER_SIZE, waitForLeader());
    }

    @Test
    void newLeaderElectedAfterLeaderFails() throws InterruptedException {
        waitForLeader();
        cluster.failNode(CLUSTER_SIZE);
        assertEquals(CLUSTER_SIZE - 1, waitForLeader());
    }

    @Test
    void recoveredNodeDoesNotBreakConsensus() throws InterruptedException {
        waitForLeader();
        cluster.failNode(CLUSTER_SIZE);
        final int leaderBeforeRecover = waitForLeader();
        assertTrue(leaderBeforeRecover > 0);
        cluster.recoverNode(CLUSTER_SIZE);
        assertEquals(CLUSTER_SIZE, waitForLeader());
    }

    @Test
    void frequentFailuresKeepConsensusStable() throws InterruptedException {
        waitForLeader();
        for (int i = 0; i < 3; i++) {
            cluster.failNode(CLUSTER_SIZE);
            waitForLeader();
            cluster.recoverNode(CLUSTER_SIZE);
        }
        assertEquals(CLUSTER_SIZE, waitForLeader());
    }

    @Test
    void leaderIsMaxAliveId() throws InterruptedException {
        waitForLeader();
        cluster.failNode(CLUSTER_SIZE);
        waitForLeader();
        cluster.failNode(CLUSTER_SIZE - 1);
        waitForLeader();
        final int leaderId = waitForLeader();
        assertEquals(cluster.getMaxAliveId(), leaderId);
        assertNotEquals(-1, leaderId);
    }
}
