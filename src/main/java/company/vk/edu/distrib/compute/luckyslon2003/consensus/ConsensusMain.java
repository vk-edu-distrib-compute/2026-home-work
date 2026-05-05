package company.vk.edu.distrib.compute.luckyslon2003.consensus;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Main class for running consensus algorithm scenarios.
 */
public final class ConsensusMain {

    private static final Logger LOGGER = Logger.getLogger(ConsensusMain.class.getName());

    /** Cluster size used across all scenarios. */
    private static final int CLUSTER_SIZE = 5;

    private ConsensusMain() {
        // Utility class - prevent instantiation
    }

    public static void main(String[] args) throws InterruptedException {
        scenario1CleanStart();
        scenario2LeaderFailure();
        scenario3NodeRecovery();
        scenario4RapidFailures();
    }

    // -----------------------------------------------------------------------
    // Scenario 1 – clean start
    // -----------------------------------------------------------------------

    private static void scenario1CleanStart() throws InterruptedException {
        printScenarioHeader(1, "Clean start – election at cluster initialisation");

        Cluster cluster = new Cluster(CLUSTER_SIZE);
        cluster.startInitialElection();

        // Wait for election to settle
        sleep(4_000);

        cluster.printStatus();
        assertSingleLeader(cluster, CLUSTER_SIZE); // highest available ID must win

        cluster.shutdown();
        printScenarioFooter(1);
    }

    // -----------------------------------------------------------------------
    // Scenario 2 – leader failure
    // -----------------------------------------------------------------------

    private static void scenario2LeaderFailure() throws InterruptedException {
        printScenarioHeader(2, "Leader failure – new leader must be elected");

        Cluster cluster = new Cluster(CLUSTER_SIZE);
        cluster.startInitialElection();
        sleep(4_000);

        cluster.printStatus();
        final int originalLeader = cluster.getConsensusLeader();
        LOGGER.info("Original leader: " + originalLeader);

        // Bring down the current leader
        cluster.getNode(originalLeader).forceDown();
        LOGGER.info("\n>>> Node " + originalLeader + " forced DOWN\n");

        // Wait for the cluster to elect a new leader
        sleep(6_000);

        cluster.printStatus();
        assertSingleLeader(cluster, originalLeader - 1); // next highest alive

        cluster.shutdown();
        printScenarioFooter(2);
    }

    // -----------------------------------------------------------------------
    // Scenario 3 – node recovery
    // -----------------------------------------------------------------------

    private static void scenario3NodeRecovery() throws InterruptedException {
        printScenarioHeader(3, "Node recovery – rejoining node must not break consensus");

        Cluster cluster = new Cluster(CLUSTER_SIZE);
        cluster.startInitialElection();
        sleep(4_000);

        cluster.printStatus();
        int originalLeader = cluster.getConsensusLeader();

        // Bring down a non-leader node
        int failingNode = 2;
        cluster.getNode(failingNode).forceDown();
        LOGGER.info("\n>>> Node " + failingNode + " forced DOWN\n");
        sleep(2_000);

        // Recover it
        cluster.getNode(failingNode).forceRecover();
        LOGGER.info("\n>>> Node " + failingNode + " forced RECOVER\n");
        sleep(4_000);

        cluster.printStatus();
        // Leader must still be the same (unless the recovered node has a higher ID)
        assertSingleLeader(cluster, originalLeader);

        cluster.shutdown();
        printScenarioFooter(3);
    }

    // -----------------------------------------------------------------------
    // Scenario 4 – rapid failures / recoveries
    // -----------------------------------------------------------------------

    private static void scenario4RapidFailures() throws InterruptedException {
        printScenarioHeader(4, "Rapid failures/recoveries – stability under chaos");

        Cluster cluster = new Cluster(CLUSTER_SIZE);
        cluster.startInitialElection();
        sleep(4_000);

        LOGGER.info("Starting rapid failure/recovery cycle...\n");

        // Cycle all non-leader nodes up and down quickly
        for (int round = 1; round <= 5; round++) {
            int nodeId = (round % (CLUSTER_SIZE - 1)) + 1; // cycle through nodes 1..4
            LOGGER.info(String.format("Round %d: taking node %d down%n", round, nodeId));
            cluster.getNode(nodeId).forceDown();
            sleep(800);

            LOGGER.info(String.format("Round %d: bringing node %d back up%n", round, nodeId));
            cluster.getNode(nodeId).forceRecover();
            sleep(1_500);

            cluster.printStatus();
            int leader = cluster.getConsensusLeader();
            LOGGER.info("Current consensus leader: " + (leader < 0 ? "NONE/SPLIT-BRAIN" : leader));
        }

        // Final stabilisation
        sleep(5_000);
        cluster.printStatus();
        assertSingleLeader(cluster, -1 /* any valid leader */);

        cluster.shutdown();
        printScenarioFooter(4);
    }

    // -----------------------------------------------------------------------
    // Assertion helpers
    // -----------------------------------------------------------------------

    /**
     * Verifies that exactly one LEADER node exists in the cluster.
     *
     * @param expectedLeaderId expected leader ID, or -1 to accept any single leader
     */
    private static void assertSingleLeader(Cluster cluster, int expectedLeaderId) {
        int actual = cluster.getConsensusLeader();

        if (actual == -2) {
            LOGGER.info("[FAIL] Split-brain detected – two or more nodes believe they are leader!");
        } else if (actual < 0) {
            LOGGER.info("[WARN] No consensus leader found yet (election still in progress?)");
        } else if (expectedLeaderId > 0 && actual != expectedLeaderId) {
            LOGGER.info(String.format("[WARN] Leader is node %d, expected node %d%n", actual, expectedLeaderId));
        } else {
            LOGGER.info(String.format("[OK]   Single leader confirmed: node %d%n", actual));
        }
    }

    // -----------------------------------------------------------------------
    // Utility
    // -----------------------------------------------------------------------

    private static void sleep(long ms) throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(ms);
    }

    private static void printScenarioHeader(int number, String description) {
        LOGGER.info("\n");
        LOGGER.info("══════════════════════════════════════════════════════");
        LOGGER.info(String.format("  SCENARIO %d: %s%n", number, description));
        LOGGER.info("══════════════════════════════════════════════════════");
    }

    private static void printScenarioFooter(int number) {
        LOGGER.info(String.format("%n  [Scenario %d complete]%n", number));
        LOGGER.info("──────────────────────────────────────────────────────\n");
    }
}