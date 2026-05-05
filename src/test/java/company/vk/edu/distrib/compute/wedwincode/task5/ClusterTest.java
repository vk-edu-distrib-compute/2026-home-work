package company.vk.edu.distrib.compute.wedwincode.task5;

import company.vk.edu.distrib.compute.wedwincode.task5.node.ClusterException;
import company.vk.edu.distrib.compute.wedwincode.task5.node.Node;
import company.vk.edu.distrib.compute.wedwincode.task5.node.State;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class ClusterTest {
    private TestCluster cluster;

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            cluster.stop();
        }
    }

    @Test
    void shouldElectHighestIdNodeOnClusterStart() {
        cluster = TestCluster.start(5);
        cluster.awaitStableLeader(5, Duration.ofSeconds(6));

        assertEquals(5, cluster.currentLeaderId());
        assertExactlyOneLeader();
        assertAllAliveNodesKnowLeader(5);
    }

    @Test
    void shouldElectNextHighestNodeAfterLeaderFailure() {
        cluster = TestCluster.start(5);
        cluster.awaitStableLeader(5, Duration.ofSeconds(6));
        cluster.node(5).setEnabled(false);
        cluster.awaitStableLeader(4, Duration.ofSeconds(6));

        assertFalse(cluster.node(5).isAlive());
        assertEquals(4, cluster.currentLeaderId());
        assertExactlyOneLeader();
        assertAllAliveNodesKnowLeader(4);
    }

    @Test
    void shouldSynchronizeRecoveredNodeWithoutBreakingConsensus() {
        cluster = TestCluster.start(5);
        cluster.awaitStableLeader(5, Duration.ofSeconds(6));
        cluster.node(5).setEnabled(false);
        cluster.awaitStableLeader(4, Duration.ofSeconds(6));
        cluster.node(5).setEnabled(true);
        cluster.awaitStableLeader(5, Duration.ofSeconds(6));

        assertTrue(cluster.node(5).isAlive());
        assertEquals(5, cluster.currentLeaderId());
        assertExactlyOneLeader();
        assertAllAliveNodesKnowLeader(5);
    }

    @Test
    void shouldHandleFrequentFailuresAndRecoveries() {
        cluster = TestCluster.start(5);
        cluster.awaitStableLeader(5, Duration.ofSeconds(6));
        cluster.node(5).setEnabled(false);
        cluster.awaitStableLeader(4, Duration.ofSeconds(6));
        cluster.node(4).setEnabled(false);
        cluster.awaitStableLeader(3, Duration.ofSeconds(6));
        cluster.node(5).setEnabled(true);
        cluster.awaitStableLeader(5, Duration.ofSeconds(6));
        cluster.node(5).setEnabled(false);
        cluster.awaitStableLeader(3, Duration.ofSeconds(6));
        cluster.node(4).setEnabled(true);
        cluster.awaitStableLeader(4, Duration.ofSeconds(6));
        cluster.node(5).setEnabled(true);
        cluster.awaitStableLeader(5, Duration.ofSeconds(6));

        assertExactlyOneLeader();
        assertAllAliveNodesKnowLeader(5);
    }

    @Test
    void gracefulShutdownShouldTransferLeadershipWithoutWaitingForTimeout() {
        cluster = TestCluster.start(5);
        cluster.awaitStableLeader(5, Duration.ofSeconds(6));

        Node leader = cluster.node(5);
        leader.gracefulShutdown();

        cluster.awaitStableLeader(4, Duration.ofSeconds(3));

        assertFalse(cluster.node(5).isAlive());
        assertEquals(4, cluster.currentLeaderId());
        assertExactlyOneLeader();
        assertAllAliveNodesKnowLeader(4);
    }

    private void assertExactlyOneLeader() {
        long leadersCount = cluster.aliveNodes().stream()
                .filter(node -> node.getState() == State.LEADER)
                .count();

        assertEquals(1, leadersCount, "There must be exactly one alive leader");
    }

    private void assertAllAliveNodesKnowLeader(int expectedLeaderId) {
        for (Node node : cluster.aliveNodes()) {
            assertEquals(
                    expectedLeaderId,
                    node.getLeaderId(),
                    "Node " + node.getId() + " has incorrect leaderId"
            );
        }
    }

    private record TestCluster(Map<Integer, Node> nodes, List<Thread> threads) {

        static TestCluster start(int size) {
            Map<Integer, Node> nodes = new ConcurrentHashMap<>();
            IntStream.rangeClosed(1, size).forEach(i -> {
                Node node = new Node(i);
                node.setRandomFailuresEnabled(false);
                nodes.put(i, node);
            });

            for (Node node : nodes.values()) {
                node.setCluster(nodes);
            }

            List<Thread> threads = new ArrayList<>();
            nodes.values().stream()
                    .map(node -> new Thread(node, "test-node-" + node.getId()))
                    .forEach(thread -> {
                        thread.start();
                        threads.add(thread);
                    });

            return new TestCluster(nodes, threads);
        }

        Node node(int id) {
            return nodes.get(id);
        }

        List<Node> aliveNodes() {
            return nodes.values().stream()
                    .filter(Node::isAlive)
                    .toList();
        }

        int currentLeaderId() {
            return aliveNodes().stream()
                    .filter(node -> node.getState() == State.LEADER)
                    .map(Node::getId)
                    .findFirst()
                    .orElse(-1);
        }

        void awaitStableLeader(int expectedLeaderId, Duration timeout) {
            long deadline = System.currentTimeMillis() + timeout.toMillis();
            AssertionError lastError = null;

            while (System.currentTimeMillis() < deadline) {
                try {
                    assertStableLeader(expectedLeaderId);
                    return;
                } catch (AssertionError error) {
                    lastError = error;
                    sleep(100);
                }
            }

            if (lastError != null) {
                throw lastError;
            }

            fail("Leader " + expectedLeaderId + " was not elected in time");
        }

        private void assertStableLeader(int expectedLeaderId) {
            Node expectedLeader = nodes.get(expectedLeaderId);
            assertNotNull(expectedLeader, "Expected leader does not exist");
            assertTrue(expectedLeader.isAlive(), "Expected leader must be alive");
            assertEquals(State.LEADER, expectedLeader.getState(), "Expected leader must have LEADER state");

            long leadersCount = aliveNodes().stream()
                    .filter(node -> node.getState() == State.LEADER)
                    .count();

            assertEquals(1, leadersCount, "Cluster must have exactly one alive leader");
            for (Node node : aliveNodes()) {
                assertEquals(
                        expectedLeaderId,
                        node.getLeaderId(),
                        "Alive node " + node.getId() + " must know leader " + expectedLeaderId
                );
            }
        }

        void stop() {
            for (Node node : nodes.values()) {
                node.stop();
            }

            for (Thread thread : threads) {
                thread.interrupt();
            }

            for (Thread thread : threads) {
                try {
                    thread.join(1_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        private static void sleep(long millis) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ClusterException("error during test", e);
            }
        }
    }
}
