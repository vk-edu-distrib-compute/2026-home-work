package company.vk.edu.distrib.compute.arseniy90.consensus;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Main {
    private static final Integer NODES_COUNT = 4;

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class.getName());

    private Main() {
    }

    public static void main(String[] args) throws InterruptedException {
        Map<Integer, Node> cluster = new ConcurrentHashMap<>();

        Node[] nodes = new Node[NODES_COUNT];
        for (int i = 0; i < NODES_COUNT; i++) {
            nodes[i] = new Node(i + 1, cluster);
        }

        for (int i = 0; i < NODES_COUNT; i++) {
            cluster.put(i + 1, nodes[i]);
        }
                             
        LOGGER.info("\nCluster initialization\n");
        for (Node node : cluster.values()) {
            node.start();
        }

        Thread.sleep(4000);
        printClusterStatus(cluster);

        LOGGER.info("\nNode crash check\n");
        Node node4 = cluster.get(4);
        node4.setRunning(false);

        Thread.sleep(6000);
        printClusterStatus(cluster);

        LOGGER.info("\nNode recovery check\n");
        node4.setRunning(true);

        Thread.sleep(4000);
        printClusterStatus(cluster);

        LOGGER.info("\nNode recovery check\n");
        node4.gracefulShutdown();

        Thread.sleep(3000);
        printClusterStatus(cluster);

        LOGGER.info("\nCluster shut down\n");
        for (Node node : cluster.values()) {
            node.shutdown();
        }

        LOGGER.info("\nFinal\n");
        System.exit(0);
    }

    private static void printClusterStatus(Map<Integer, Node> cluster) {
        StringBuilder sb = new StringBuilder(128);
        sb.append("\n---Node current statuses---\n");
        for (int id : cluster.keySet()) {
            Node node = cluster.get(id);
            sb.append("Node ").append(id)
              .append(" | Role: ").append(node.getRole())
              .append(" | Leader: ").append(id == node.getLeaderId()).append('\n');
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("{}", sb.toString());
        }
    }
}
