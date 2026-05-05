package company.vk.edu.distrib.compute.nihuaway00.task5;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.fail;

class NodeRegistryTest {
    private static final int NODE_COUNT = 5;
    private static final long CLUSTER_TIMEOUT_MS = 8_000;
    private static final long POLL_MS = 50;
    private NodeRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new NodeRegistry();
        registry.generateNodes(NODE_COUNT);
        registry.runAll();
    }

    @AfterEach
    void tearDown() {
        registry.stopAll();
    }

    @Test
    void electsHighestNodeOnStartup() {
        assertEventuallyConsistentLeader(4);
    }

    @Test
    void reElectsLeaderAfterCurrentLeaderFailure() {
        assertEventuallyConsistentLeader(4);

        Node leader = registry.getNode(4);
        leader.disable();

        assertEventuallyConsistentLeader(3);
    }

    @Test
    void recoveredHighestNodeSynchronizesAndBecomesLeaderAgain() {
        assertEventuallyConsistentLeader(4);

        Node leader = registry.getNode(4);
        leader.disable();
        assertEventuallyConsistentLeader(3);

        leader.enable();
        assertEventuallyConsistentLeader(4);
    }

    @Test
    void remainsStableUnderFrequentFailuresAndRecoveries() {
        assertEventuallyConsistentLeader(4);

        for (int step = 0; step < 12; step++) {
            int nodeId = step % 3;
            Node node = registry.getNode(nodeId);

            node.disable();
            assertEventuallyConsistentLeader(4);

            node.enable();
            assertEventuallyConsistentLeader(4);
        }
    }

    private void assertEventuallyConsistentLeader(int expectedLeaderId) {
        assertEventuallyConsistentLeader(expectedLeaderId, CLUSTER_TIMEOUT_MS);
    }

    private void assertEventuallyConsistentLeader(int expectedLeaderId, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (isClusterConsistent(expectedLeaderId)) {
                return;
            }
            try {
                Thread.sleep(POLL_MS);
            } catch (InterruptedException expected) {
                Thread.currentThread().interrupt();
                fail(expected);
            }
        }
        fail("Cluster is not consistent for leader " + expectedLeaderId + ". Actual leaders: " + registry.getLeaderIds());
    }

    private boolean isClusterConsistent(int expectedLeaderId) {
        List<Node> aliveNodes = registry.getAllNodes().stream().filter(Node::isAlive).toList();
        if (aliveNodes.isEmpty()) {
            return false;
        }

        long leaderCount = aliveNodes.stream().filter(node -> node.getState() == NodeState.LEADER).count();
        boolean expectedLeaderAlive = aliveNodes.stream().anyMatch(node -> node.getId() == expectedLeaderId);
        boolean allHaveSameLeader = aliveNodes.stream().allMatch(node -> node.getCurrentLeaderId() == expectedLeaderId);

        return expectedLeaderAlive && leaderCount == 1 && allHaveSameLeader;
    }
}
