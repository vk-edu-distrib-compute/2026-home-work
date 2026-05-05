package company.vk.edu.distrib.compute.nst1610.consensus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

class ConsensusClusterTest {
    @Test
    void shouldElectHighestNodeOnStartup() throws Exception {
        try (ConsensusCluster cluster = ConsensusCluster.withNodeCount(5, ConsensusConfig.defaultConfig())) {
            cluster.start();
            waitUntil(Duration.ofSeconds(3), () -> cluster.snapshots().stream()
                .filter(snapshot -> !snapshot.failed() && snapshot.leader())
                .count() == 1);
            assertClusterConsistent(cluster);
            assertEquals(5, cluster.getHighestAliveNodeId());
            assertEquals(5, currentLeader(cluster.snapshots()));
        }
    }

    @Test
    void shouldElectNextHighestNodeAfterLeaderFailure() throws Exception {
        try (ConsensusCluster cluster = ConsensusCluster.withNodeCount(5, ConsensusConfig.defaultConfig())) {
            cluster.start();
            waitUntil(Duration.ofSeconds(3), () -> currentLeader(cluster.snapshots()) == 5);
            cluster.failNode(5);
            waitUntil(Duration.ofSeconds(3), () -> currentLeader(cluster.snapshots()) == 4);
            assertClusterConsistent(cluster);
            assertEquals(4, cluster.getHighestAliveNodeId());
        }
    }

    @Test
    void shouldRestoreHighestLeaderAfterRecovery() throws Exception {
        try (ConsensusCluster cluster = ConsensusCluster.withNodeCount(5, ConsensusConfig.defaultConfig())) {
            cluster.start();
            waitUntil(Duration.ofSeconds(3), () -> currentLeader(cluster.snapshots()) == 5);
            cluster.failNode(5);
            waitUntil(Duration.ofSeconds(3), () -> currentLeader(cluster.snapshots()) == 4);
            cluster.recoverNode(5);
            waitUntil(Duration.ofSeconds(3), () -> currentLeader(cluster.snapshots()) == 5);
            assertClusterConsistent(cluster);
        }
    }

    @Test
    void shouldRemainConsistentDuringFrequentFailures() throws Exception {
        try (ConsensusCluster cluster = ConsensusCluster.withNodeCount(5, ConsensusConfig.unstableConfig())) {
            cluster.start();
            Thread.sleep(4_500L);
            assertClusterConsistent(cluster);
        }
    }

    private static void assertClusterConsistent(ConsensusCluster cluster) {
        List<NodeSnapshot> snapshots = cluster.snapshots();
        int expectedLeader = snapshots.stream()
            .filter(snapshot -> !snapshot.failed())
            .mapToInt(NodeSnapshot::nodeId)
            .max()
            .orElse(-1);
        long actualLeaders = snapshots.stream()
            .filter(snapshot -> !snapshot.failed() && snapshot.leader())
            .count();
        assertTrue(actualLeaders <= 1, "More than one active leader found");
        if (expectedLeader < 0) {
            return;
        }
        assertEquals(expectedLeader, currentLeader(snapshots));
        for (NodeSnapshot snapshot : snapshots) {
            if (!snapshot.failed()) {
                assertEquals(expectedLeader, snapshot.knownLeaderId());
            }
        }
    }

    private static int currentLeader(List<NodeSnapshot> snapshots) {
        return snapshots.stream()
            .filter(snapshot -> !snapshot.failed() && snapshot.leader())
            .mapToInt(NodeSnapshot::nodeId)
            .findFirst()
            .orElse(-1);
    }

    private static void waitUntil(Duration timeout, BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(50L);
        }
        assertTrue(condition.getAsBoolean(), "Condition was not met within " + timeout);
    }
}
