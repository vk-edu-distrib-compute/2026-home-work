package company.vk.edu.distrib.compute.luckyslon2003.consensus;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class ConsensusMain {

    private static final Logger LOGGER = Logger.getLogger(ConsensusMain.class.getName());

    private static final int CLUSTER_SIZE = 5;

    private ConsensusMain() {
    }

    public static void main(String[] args) throws InterruptedException {
        scenario1CleanStart();
        scenario2LeaderFailure();
        scenario3NodeRecovery();
        scenario4RapidFailures();
    }

    private static void scenario1CleanStart() throws InterruptedException {
        printScenarioHeader(1, "Clean start – election at cluster initialisation");

        Cluster cluster = new Cluster(CLUSTER_SIZE);
        cluster.startInitialElection();

        sleep(4_000);

        cluster.printStatus();
        assertSingleLeader(cluster, CLUSTER_SIZE);

        cluster.shutdown();
        printScenarioFooter(1);
    }

    private static void scenario2LeaderFailure() throws InterruptedException {
        printScenarioHeader(2, "Leader failure – new leader must be elected");

        Cluster cluster = new Cluster(CLUSTER_SIZE);
        cluster.startInitialElection();
        sleep(4_000);

        cluster.printStatus();
        final int originalLeader = cluster.getConsensusLeader();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Original leader: " + originalLeader);
        }

        cluster.getNode(originalLeader).forceDown();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("\n>>> Node " + originalLeader + " forced DOWN\n");
        }

        sleep(6_000);

        cluster.printStatus();
        assertSingleLeader(cluster, originalLeader - 1);

        cluster.shutdown();
        printScenarioFooter(2);
    }

    private static void scenario3NodeRecovery() throws InterruptedException {
        printScenarioHeader(3, "Node recovery – rejoining node must not break consensus");

        Cluster cluster = new Cluster(CLUSTER_SIZE);
        cluster.startInitialElection();
        sleep(4_000);

        cluster.printStatus();
        final int originalLeader = cluster.getConsensusLeader();

        int failingNode = 2;
        cluster.getNode(failingNode).forceDown();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("\n>>> Node " + failingNode + " forced DOWN\n");
        }
        sleep(2_000);

        cluster.getNode(failingNode).forceRecover();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("\n>>> Node " + failingNode + " forced RECOVER\n");
        }
        sleep(4_000);

        cluster.printStatus();
        assertSingleLeader(cluster, originalLeader);

        cluster.shutdown();
        printScenarioFooter(3);
    }

    private static void scenario4RapidFailures() throws InterruptedException {
        printScenarioHeader(4, "Rapid failures/recoveries – stability under chaos");

        Cluster cluster = new Cluster(CLUSTER_SIZE);
        cluster.startInitialElection();
        sleep(4_000);

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting rapid failure/recovery cycle...\n");
        }

        for (int round = 1; round <= 5; round++) {
            int nodeId = (round % (CLUSTER_SIZE - 1)) + 1;
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info(String.format("Round %d: taking node %d down%n", round, nodeId));
            }
            cluster.getNode(nodeId).forceDown();
            sleep(800);

            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info(String.format("Round %d: bringing node %d back up%n", round, nodeId));
            }
            cluster.getNode(nodeId).forceRecover();
            sleep(1_500);

            cluster.printStatus();
            int leader = cluster.getConsensusLeader();
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Current consensus leader: " + (leader < 0 ? "NONE/SPLIT-BRAIN" : leader));
            }
        }

        sleep(5_000);
        cluster.printStatus();
        assertSingleLeader(cluster, -1);

        cluster.shutdown();
        printScenarioFooter(4);
    }

    private static void assertSingleLeader(Cluster cluster, int expectedLeaderId) {
        int actual = cluster.getConsensusLeader();

        if (actual == -2) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("[FAIL] Split-brain detected – two or more nodes believe they are leader!");
            }
        } else if (actual < 0) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("[WARN] No consensus leader found yet (election still in progress?)");
            }
        } else if (expectedLeaderId > 0 && actual != expectedLeaderId) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info(String.format("[WARN] Leader is node %d, expected node %d%n", actual, expectedLeaderId));
            }
        } else {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info(String.format("[OK]   Single leader confirmed: node %d%n", actual));
            }
        }
    }

    private static void sleep(long ms) throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(ms);
    }

    private static void printScenarioHeader(int number, String description) {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("\n");
            LOGGER.info("══════════════════════════════════════════════════════");
            LOGGER.info(String.format("  SCENARIO %d: %s%n", number, description));
            LOGGER.info("══════════════════════════════════════════════════════");
        }
    }

    private static void printScenarioFooter(int number) {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(String.format("%n  [Scenario %d complete]%n", number));
            LOGGER.info("──────────────────────────────────────────────────────\n");
        }
    }
}
