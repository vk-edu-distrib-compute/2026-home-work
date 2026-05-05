package company.vk.edu.distrib.compute.andeco.election;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings({"PMD.UnitTestAssertionsShouldIncludeMessage", "PMD.UnitTestContainsTooManyAsserts"})
class LeaderElectionTest {

    @Test
    void startupElectsMaxIdLeader() throws InterruptedException {
        Cluster cluster = startCluster(5, 1500, 500, 1200);
        try {
            waitForStableLeader(cluster, 5000);
            assertEquals(5, requireLeader(cluster));
            assertNoSplitBrain(cluster);
        } finally {
            stopCluster(cluster);
        }
    }

    @Test
    void singleNodeElectsItself() throws InterruptedException {
        Cluster cluster = startCluster(1, 800, 200, 600);
        try {
            waitForStableLeader(cluster, 3000);
            assertEquals(1, requireLeader(cluster));
            assertNoSplitBrain(cluster);
        } finally {
            stopCluster(cluster);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3, 5, 9})
    void startupAlwaysChoosesMaxAlive(int n) throws InterruptedException {
        Cluster cluster = startCluster(n, 1200, 400, 900);
        try {
            waitForStableLeader(cluster, 5000);
            assertEquals(expectedLeader(cluster), requireLeader(cluster));
            assertNoSplitBrain(cluster);
        } finally {
            stopCluster(cluster);
        }
    }

    @Test
    void leaderCrashElectsNextMaxAlive() throws InterruptedException {
        Cluster cluster = startCluster(5, 1500, 500, 1200);
        try {
            waitForStableLeader(cluster, 5000);
            assertEquals(5, requireLeader(cluster));

            cluster.disableNode(5);
            waitForStableLeader(cluster, 5000);
            assertEquals(4, requireLeader(cluster));
            assertNoSplitBrain(cluster);
        } finally {
            stopCluster(cluster);
        }
    }

    @Test
    void leaderRecoveryDoesNotBreakConsistencyAndCanReclaimLeadership() throws InterruptedException {
        Cluster cluster = startCluster(5, 1500, 500, 1200);
        try {
            waitForStableLeader(cluster, 5000);
            assertEquals(5, requireLeader(cluster));

            cluster.disableNode(5);
            waitForStableLeader(cluster, 5000);
            assertEquals(4, requireLeader(cluster));
            assertNoSplitBrain(cluster);

            cluster.enableNode(5);
            waitForStableLeader(cluster, 5000);
            assertEquals(5, requireLeader(cluster));
            assertNoSplitBrain(cluster);
        } finally {
            stopCluster(cluster);
        }
    }

    @Test
    void cascadingFailuresPickNextMaxEachTime() throws InterruptedException {
        Cluster cluster = startCluster(6, 1500, 500, 1200);
        try {
            waitForStableLeader(cluster, 5000);
            assertEquals(6, requireLeader(cluster));

            cluster.disableNode(6);
            waitForStableLeader(cluster, 5000);
            assertEquals(5, requireLeader(cluster));

            cluster.disableNode(5);
            waitForStableLeader(cluster, 5000);
            assertEquals(4, requireLeader(cluster));

            assertNoSplitBrain(cluster);
        } finally {
            stopCluster(cluster);
        }
    }

    @Test
    void gracefulShutdownLeaderTriggersReelection() throws InterruptedException {
        Cluster cluster = startCluster(5, 1500, 500, 1200);
        try {
            waitForStableLeader(cluster, 5000);
            assertEquals(5, requireLeader(cluster));

            cluster.gracefulShutdownLeader();
            waitForStableLeader(cluster, 5000);
            assertEquals(4, requireLeader(cluster));
            assertNoSplitBrain(cluster);
        } finally {
            stopCluster(cluster);
        }
    }

    @Test
    void allNodesDownMeansNoLeader() throws InterruptedException {
        Cluster cluster = startCluster(4, 800, 250, 700);
        try {
            waitForStableLeader(cluster, 4000);
            assertNotNull(currentSingleLeaderOrNull(cluster));

            for (ElectionNode node : cluster.nodes()) {
                cluster.disableNode(node.id());
            }

            waitForNoLeader(cluster, 2000);
            assertNull(currentSingleLeaderOrNull(cluster));
        } finally {
            stopCluster(cluster);
        }
    }

    @Test
    void staleMessagesDoNotChangeLeader() throws InterruptedException {
        Cluster cluster = startCluster(5, 1500, 500, 1200);
        try {
            waitForStableLeader(cluster, 5000);
            int leader = requireLeader(cluster);
            assertEquals(expectedLeader(cluster), leader);

            cluster.broadcast(new Message(MessageType.VICTORY, 1, 0));
            cluster.broadcast(new Message(MessageType.STEP_DOWN, leader, 0));
            TimeUnit.MILLISECONDS.sleep(300);

            assertEquals(leader, requireLeader(cluster));
            assertNoSplitBrain(cluster);
        } finally {
            stopCluster(cluster);
        }
    }

    @Test
    void frequentFailuresDoesNotCreateTwoLeaders() throws InterruptedException {
        Cluster cluster = startCluster(7, 1500, 500, 1200);
        try {
            waitForStableLeader(cluster, 5000);
            assertNoSplitBrain(cluster);

            int maxId = cluster.nodes().stream().map(ElectionNode::id).max(Comparator.naturalOrder()).orElse(1);
            for (int i = 0; i < 6; i++) {
                cluster.disableNode(maxId);
                waitForStableLeader(cluster, 5000);
                assertNoSplitBrain(cluster);

                cluster.enableNode(maxId);
                waitForStableLeader(cluster, 5000);
                assertNoSplitBrain(cluster);
            }
        } finally {
            stopCluster(cluster);
        }
    }

    private static Cluster startCluster(int n,
                                        long pingTimeoutMs,
                                        long answerTimeoutMs,
                                        long victoryTimeoutMs) {
        List<Integer> ids = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            ids.add(i);
        }

        List<ElectionNode> nodes = new ArrayList<>();
        ElectionConfig config = ElectionConfig.defaults(pingTimeoutMs, answerTimeoutMs, victoryTimeoutMs);
        for (int id : ids) {
            nodes.add(new ElectionNode(id, ids, config, pingTimeoutMs, answerTimeoutMs, victoryTimeoutMs,
                    0.0, 100, 200, 1));
        }

        Cluster cluster = new Cluster(nodes);
        for (ElectionNode node : nodes) {
            node.attachCluster(cluster);
        }
        nodes.forEach(Thread::start);
        return cluster;
    }

    private static void stopCluster(Cluster cluster) throws InterruptedException {
        for (ElectionNode node : cluster.nodes()) {
            node.shutdown();
        }
        for (ElectionNode node : cluster.nodes()) {
            node.join(1500);
        }
    }

    private static void waitForStableLeader(Cluster cluster, long timeoutMs) throws InterruptedException {
        long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        Integer last = null;
        int sameCount = 0;

        while (System.nanoTime() < until) {
            Integer leader = currentSingleLeaderOrNull(cluster);
            if (leader != null && leader.equals(last)) {
                sameCount++;
                if (sameCount >= 3) {
                    return;
                }
            } else {
                sameCount = 0;
                last = leader;
            }
            TimeUnit.MILLISECONDS.sleep(150);
        }
    }

    private static void waitForNoLeader(Cluster cluster, long timeoutMs) throws InterruptedException {
        long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (System.nanoTime() < until) {
            if (currentSingleLeaderOrNull(cluster) == null) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(100);
        }
    }

    private static int requireLeader(Cluster cluster) {
        Integer leader = currentSingleLeaderOrNull(cluster);
        assertNotNull(leader);
        return leader;
    }

    private static void assertNoSplitBrain(Cluster cluster) {
        int leaders = 0;
        Integer leaderId = null;
        for (ElectionNode node : cluster.nodes()) {
            if (node.role() == NodeRole.LEADER) {
                leaders++;
                if (leaderId == null) {
                    leaderId = node.id();
                } else {
                    assertEquals(leaderId.intValue(), node.id());
                }
            }
        }
        assertTrue(leaders <= 1);
    }

    private static int expectedLeader(Cluster cluster) {
        return cluster.nodes().stream()
                .filter(n -> n.role() != NodeRole.DOWN)
                .map(ElectionNode::id)
                .max(Comparator.naturalOrder())
                .orElseThrow();
    }

    private static Integer currentSingleLeaderOrNull(Cluster cluster) {
        Integer leader = null;
        for (ElectionNode node : cluster.nodes()) {
            if (node.role() == NodeRole.LEADER) {
                if (leader != null && leader != node.id()) {
                    return null;
                }
                leader = node.id();
            }
        }
        return leader;
    }
}

