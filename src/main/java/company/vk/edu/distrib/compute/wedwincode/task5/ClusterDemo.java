package company.vk.edu.distrib.compute.wedwincode.task5;

import java.util.LinkedHashMap;
import java.util.Map;

public class ClusterDemo {
    static void main() {
        try {
            Map<Integer, Node> cluster = new LinkedHashMap<>();

            for (int i = 1; i <= 5; i++) {
                cluster.put(i, new Node(i));
            }

            for (Node node : cluster.values()) {
                node.setCluster(cluster);
            }

            for (Node node : cluster.values()) {
                new Thread(node, "node-" + node.getId()).start();
            }

            ClusterMonitor clusterMonitor = new ClusterMonitor(cluster);
            new Thread(clusterMonitor, "clusterMonitor").start();

            Thread.sleep(5_000);

            System.out.println("\n--- Graceful shutdown current leader ---");
            Node leader = findCurrentLeader(cluster);

            if (leader != null) {
                leader.gracefulShutdown();
            }

            Thread.sleep(7_000);

            System.out.println("\n--- Enable node 5 ---");
            cluster.get(5).setEnabled(true);

            Thread.sleep(10_000);

            System.out.println("\n--- Final cluster state ---");
            ClusterLogger.clusterSnapshot(cluster);

            clusterMonitor.stop();
            for (Node node : cluster.values()) {
                node.stop();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("thread was interrupted");
        }
    }

    private static Node findCurrentLeader(Map<Integer, Node> cluster) {
        for (Node node : cluster.values()) {
            if (node.isAlive() && node.getState() == State.LEADER) {
                return node;
            }
        }

        return null;
    }
}