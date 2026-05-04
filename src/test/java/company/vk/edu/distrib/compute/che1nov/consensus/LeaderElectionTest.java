package company.vk.edu.distrib.compute.che1nov.consensus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LeaderElectionTest {
    private static final int SINGLE_LEADER_COUNT = 1;
    private ClusterModel cluster;

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            cluster.stop();
        }
    }

    @Test
    void startupElectsMaxIdLeader() {
        cluster = createCluster(0.0d);
        cluster.start();

        waitForSingleLeader();

        assertEquals(5, cluster.leaders().getFirst().nodeId());
        assertNoSplitBrain();
    }

    @Test
    void leaderFailureElectsNextMaxAlive() {
        cluster = createCluster(0.0d);
        cluster.start();
        waitForSingleLeader();

        cluster.forceDown(5);

        waitForSingleLeader();
        assertEquals(4, cluster.leaders().getFirst().nodeId());
        assertNoSplitBrain();
    }

    @Test
    void recoveredNodeDoesNotBreakConsensus() {
        cluster = createCluster(0.0d);
        cluster.start();
        waitForSingleLeader();

        cluster.forceDown(5);
        waitForSingleLeader();
        assertEquals(4, cluster.leaders().getFirst().nodeId());

        cluster.forceUp(5);
        waitForSingleLeader();
        assertEquals(5, cluster.leaders().getFirst().nodeId());
        assertNoSplitBrain();
    }

    @Test
    void frequentFailuresKeepSingleLeaderAmongAliveNodes() {
        cluster = createCluster(0.0d);
        cluster.start();
        waitForSingleLeader();

        for (int i = 0; i < 5; i++) {
            int victim = 5 - (i % 3);
            cluster.forceDown(victim);
            sleep(450);
            cluster.forceUp(victim);
            sleep(450);

            waitForSingleLeader();
            Integer expected = cluster.maxLiveNodeId();
            assertEquals(expected, cluster.leaders().getFirst().nodeId());
            assertNoSplitBrain();
        }
    }

    private ClusterModel createCluster(double failProbability) {
        return new ClusterModel(
                List.of(1, 2, 3, 4, 5),
                Duration.ofMillis(120),
                Duration.ofMillis(240),
                Duration.ofMillis(260),
                failProbability,
                Duration.ofMillis(300),
                Duration.ofMillis(700)
        );
    }

    private void waitForSingleLeader() {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            List<ClusterNode> leaders = cluster.leaders();
            if (leaders.size() == SINGLE_LEADER_COUNT) {
                Integer expected = cluster.maxLiveNodeId();
                if (expected != null && leaders.getFirst().nodeId() == expected) {
                    return;
                }
            }
            sleep(50);
        }
        throw new AssertionError("Single leader not established in time: " + cluster.snapshot());
    }

    private void assertNoSplitBrain() {
        assertTrue(cluster.leaders().size() <= 1, "Split-brain detected: " + cluster.snapshot());
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted", e);
        }
    }
}
