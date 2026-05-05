package company.vk.edu.distrib.compute.vladislavguzov.consensus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class ConsensusTest {

    private static final int CLUSTER_SIZE = 5;
    private static final long CONVERGENCE_TIMEOUT_MS = 10_000;

    private Cluster cluster;

    @BeforeEach
    void setUp() {
        cluster = new Cluster(CLUSTER_SIZE);
    }

    @AfterEach
    void tearDown() {
        cluster.stop();
    }

    @Test
    void startupElectsExactlyOneLeader() throws InterruptedException {
        cluster.start();
        waitForConsensus();
        assertSingleLeader();
        assertEquals(CLUSTER_SIZE - 1, cluster.getCurrentLeaderId());
    }

    @Test
    void leaderFailureTriggersReelection() throws InterruptedException {
        cluster.start();
        waitForConsensus();

        int oldLeader = cluster.getCurrentLeaderId();
        cluster.getNode(oldLeader).forceDown();

        waitForConsensus();

        int newLeader = cluster.getCurrentLeaderId();
        assertNotEquals(oldLeader, newLeader);
        assertTrue(newLeader >= 0);
        assertSingleLeader();
    }

    @Test
    void recoveredNodeDoesNotDisruptConsensus() throws InterruptedException {
        cluster.start();
        waitForConsensus();

        int followerId = findFollower();
        cluster.getNode(followerId).forceDown();
        Thread.sleep(500);
        cluster.getNode(followerId).forceUp();

        waitForConsensus();
        assertSingleLeader();
    }

    @Test
    void frequentFailuresMaintainConsensus() throws InterruptedException {
        cluster.start();
        waitForConsensus();

        Random rng = new Random(42);
        for (int i = 0; i < 10; i++) {
            int nodeId = rng.nextInt(CLUSTER_SIZE);
            cluster.getNode(nodeId).forceDown();
            Thread.sleep(200);
            cluster.getNode(nodeId).forceUp();
            Thread.sleep(300);
        }

        waitForConsensus();
        assertSingleLeader();
    }

    private void waitForConsensus() throws InterruptedException {
        long deadline = System.currentTimeMillis() + CONVERGENCE_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (isStable()) {
                return;
            }
            Thread.sleep(100);
        }
        fail("Consensus not reached within " + CONVERGENCE_TIMEOUT_MS + " ms. State: " + clusterSnapshot());
    }

    private boolean isStable() throws InterruptedException {
        if (!isConsensusReached()) {
            return false;
        }
        Thread.sleep(200);
        return isConsensusReached();
    }

    private boolean isConsensusReached() {
        int leaderCount = 0;
        int leaderNodeId = -1;
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            NodeState st = cluster.getNode(i).getNodeState();
            if (st == NodeState.CANDIDATE) {
                return false;
            }
            if (st == NodeState.LEADER) {
                leaderCount++;
                leaderNodeId = i;
            }
        }
        if (leaderCount != 1) {
            return false;
        }
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            Node node = cluster.getNode(i);
            if (node.getNodeState() == NodeState.FOLLOWER && node.getLeaderId() != leaderNodeId) {
                return false;
            }
        }
        return true;
    }

    private void assertSingleLeader() {
        int leaderCount = 0;
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            if (cluster.getNode(i).getNodeState() == NodeState.LEADER) {
                leaderCount++;
            }
        }
        assertEquals(1, leaderCount);
    }

    private int findFollower() {
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            if (cluster.getNode(i).getNodeState() == NodeState.FOLLOWER) {
                return i;
            }
        }
        return 0;
    }

    private String clusterSnapshot() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            Node node = cluster.getNode(i);
            sb.append(String.format("Node%d[%s,leader=%d] ", i, node.getNodeState(), node.getLeaderId()));
        }
        return sb.toString().trim();
    }
}
