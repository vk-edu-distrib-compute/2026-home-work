package company.vk.edu.distrib.compute.luckyslon2003.consensus;

import java.util.concurrent.TimeUnit;


public class ConsensusMain {

    /** Cluster size used across all scenarios. */
    private static final int CLUSTER_SIZE = 5;

    public static void main(String[] args) throws InterruptedException {
        scenario1_cleanStart();
        scenario2_leaderFailure();
        scenario3_nodeRecovery();
        scenario4_rapidFailures();
    }

    // -----------------------------------------------------------------------
    // Scenario 1 – clean start
    // -----------------------------------------------------------------------

    private static void scenario1_cleanStart() throws InterruptedException {
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

    private static void scenario2_leaderFailure() throws InterruptedException {
        printScenarioHeader(2, "Leader failure – new leader must be elected");

        Cluster cluster = new Cluster(CLUSTER_SIZE);
        cluster.startInitialElection();
        sleep(4_000);

        cluster.printStatus();
        int originalLeader = cluster.getConsensusLeader();
        System.out.println("Original leader: " + originalLeader);

        // Bring down the current leader
        cluster.getNode(originalLeader).forceDown();
        System.out.println("\n>>> Node " + originalLeader + " forced DOWN\n");

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

    private static void scenario3_nodeRecovery() throws InterruptedException {
        printScenarioHeader(3, "Node recovery – rejoining node must not break consensus");

        Cluster cluster = new Cluster(CLUSTER_SIZE);
        cluster.startInitialElection();
        sleep(4_000);

        cluster.printStatus();
        int originalLeader = cluster.getConsensusLeader();

        // Bring down a non-leader node
        int failingNode = 2;
        cluster.getNode(failingNode).forceDown();
        System.out.println("\n>>> Node " + failingNode + " forced DOWN\n");
        sleep(2_000);

        // Recover it
        cluster.getNode(failingNode).forceRecover();
        System.out.println("\n>>> Node " + failingNode + " forced RECOVER\n");
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

    private static void scenario4_rapidFailures() throws InterruptedException {
        printScenarioHeader(4, "Rapid failures/recoveries – stability under chaos");

        Cluster cluster = new Cluster(CLUSTER_SIZE);
        cluster.startInitialElection();
        sleep(4_000);

        System.out.println("Starting rapid failure/recovery cycle...\n");

        // Cycle all non-leader nodes up and down quickly
        for (int round = 1; round <= 5; round++) {
            int nodeId = (round % (CLUSTER_SIZE - 1)) + 1; // cycle through nodes 1..4
            System.out.printf("Round %d: taking node %d down%n", round, nodeId);
            cluster.getNode(nodeId).forceDown();
            sleep(800);

            System.out.printf("Round %d: bringing node %d back up%n", round, nodeId);
            cluster.getNode(nodeId).forceRecover();
            sleep(1_500);

            cluster.printStatus();
            int leader = cluster.getConsensusLeader();
            System.out.println("Current consensus leader: " + (leader < 0 ? "NONE/SPLIT-BRAIN" : leader));
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
            System.out.println("[FAIL] Split-brain detected – two or more nodes believe they are leader!");
        } else if (actual < 0) {
            System.out.println("[WARN] No consensus leader found yet (election still in progress?)");
        } else if (expectedLeaderId > 0 && actual != expectedLeaderId) {
            System.out.printf("[WARN] Leader is node %d, expected node %d%n", actual, expectedLeaderId);
        } else {
            System.out.printf("[OK]   Single leader confirmed: node %d%n", actual);
        }
    }

    // -----------------------------------------------------------------------
    // Utility
    // -----------------------------------------------------------------------

    private static void sleep(long ms) throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(ms);
    }

    private static void printScenarioHeader(int number, String description) {
        System.out.println("\n");
        System.out.println("══════════════════════════════════════════════════════");
        System.out.printf("  SCENARIO %d: %s%n", number, description);
        System.out.println("══════════════════════════════════════════════════════");
    }

    private static void printScenarioFooter(int number) {
        System.out.printf("%n  [Scenario %d complete]%n", number);
        System.out.println("──────────────────────────────────────────────────────\n");
    }
}
