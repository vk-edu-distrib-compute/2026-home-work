package company.vk.edu.distrib.compute.artttnik;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;

public final class Main {
    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());
    private static final String NODE_PREFIX = "Node ";
    private static final String LEADER_PREFIX = " leader=";

    private Main() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static void main(String[] args) {
        LOGGER.info("Starting leader election simulation for artttnik");

        int n = 5;
        double failProb = 0.0;
        long tick = 200;
        long pingInterval = 500;
        long electionTimeout = 800;

        Cluster cluster = new Cluster();
        initializeCluster(cluster, n, failProb, tick, pingInterval, electionTimeout);

        cluster.startAll();
        Monitor monitor = new Monitor(cluster, 500);
        Thread monitorThread = new Thread(monitor, "Monitor");
        monitorThread.start();

        try {
            runScenarios(cluster, n);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.log(Level.WARNING, "Main interrupted", e);
        } finally {
            cleanupMonitor(monitor, monitorThread);
        }
    }

    private static void initializeCluster(Cluster cluster, int n,
                                          double failProb, long tick, long pingInterval, long electionTimeout) {
        IntStream.rangeClosed(1, n)
                .mapToObj(i -> new Node(i, cluster, failProb, tick, pingInterval, electionTimeout))
                .forEach(cluster::addNode);
    }

    private static void runScenarios(Cluster cluster, int n) throws InterruptedException {
        Thread.sleep(2000);
        printInitialState(cluster);

        int leaderId = n;
        testLeaderCrash(cluster, leaderId);
        testLeaderRecovery(cluster, leaderId);
        testFlapping(cluster, n);

        Thread.sleep(2000);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("\n=== Final state ===");
            cluster.nodeIds().forEach(id -> LOGGER.info(NODE_PREFIX
                    + id + LEADER_PREFIX + cluster.getNode(id).getLeaderId()));
        }

        cluster.shutdownAll();
    }

    private static void printInitialState(Cluster cluster) {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("=== Initial stable state ===");
            cluster.nodeIds().forEach(id -> LOGGER.info(NODE_PREFIX
                    + id + LEADER_PREFIX + cluster.getNode(id).getLeaderId()));
        }
    }

    private static void testLeaderCrash(Cluster cluster, int leaderId) throws InterruptedException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("\n=== Crashing leader " + leaderId + " ===");
        }
        cluster.getNode(leaderId).forceDown();
        Thread.sleep(2000);
        if (LOGGER.isLoggable(Level.INFO)) {
            cluster.nodeIds().forEach(id -> LOGGER.info(NODE_PREFIX
                    + id + " leader=" + cluster.getNode(id).getLeaderId()));
        }
    }

    private static void testLeaderRecovery(Cluster cluster, int leaderId) throws InterruptedException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("\n=== Recovering leader " + leaderId + " ===");
        }
        cluster.getNode(leaderId).forceUp();
        Thread.sleep(2000);
        if (LOGGER.isLoggable(Level.INFO)) {
            cluster.nodeIds().forEach(id -> LOGGER.info(NODE_PREFIX
                    + id + " leader=" + cluster.getNode(id).getLeaderId()));
        }
    }

    private static void testFlapping(Cluster cluster, int n) throws InterruptedException {
        LOGGER.info("\n=== Flapping test (random failures) ===");
        cluster.getNode(n).forceUp();

        Thread flapper = new Thread(() -> {
            try {
                for (int i = 0; i < 10; i++) {
                    Thread.sleep(700);
                    int pick = 1 + (int) (Math.random() * n);
                    Node nn = cluster.getNode(pick);
                    if (nn != null) {
                        toggleNode(nn);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        flapper.start();
        flapper.join();
    }

    private static void toggleNode(Node node) {
        if (node.isAliveNode()) {
            node.forceDown();
        } else {
            node.forceUp();
        }
    }

    private static void cleanupMonitor(Monitor monitor, Thread monitorThread) {
        LOGGER.info("Simulation finished");
        monitor.stop();
        monitorThread.interrupt();
        try {
            monitorThread.join(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
