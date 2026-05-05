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

            System.out.println("\n--- Disable node 5 ---");
            cluster.get(5).setEnabled(false);

            Thread.sleep(7_000);

            System.out.println("\n--- Enable node 5 ---");
            cluster.get(5).setEnabled(true);

            Thread.sleep(10_000);

            for (Node node : cluster.values()) {
                node.stop();
            }
            clusterMonitor.stop();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("thread was interrupted");
        }
    }
}