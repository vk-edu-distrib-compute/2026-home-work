package company.vk.edu.distrib.compute.linempy.task5;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BullyAlgorithmSystem {
    public static void main(String[] args) throws InterruptedException {
        int nodeCount = 5;
        Map<Integer, Node> nodes = new ConcurrentHashMap<>();
        ExecutorService threadPool = Executors.newFixedThreadPool(nodeCount);

        System.out.println("=== INITIALIZING CLUSTER WITH " + nodeCount + " NODES ===\n");

        for (int i = 1; i <= nodeCount; i++) {
            nodes.put(i, new Node(i, nodes));
        }

        nodes.values().forEach(threadPool::submit);

        Thread.sleep(5000);
        printStatus(nodes);

        // TEST 1: LEADER FAILURE (Node 5)
        System.out.println("\n--- TEST 1: LEADER FAILURE (Node 5) ---");
        nodes.get(5).setStatus(false);
        Thread.sleep(8000);
        printStatus(nodes);

        // TEST 2: NODE RECOVERY (Node 5)
        System.out.println("\n--- TEST 2: NODE RECOVERY (Node 5) ---");
        nodes.get(5).setStatus(true);
        Thread.sleep(8000);
        printStatus(nodes);

        // TEST 3: GRACEFUL SHUTDOWN
        System.out.println("\n--- TEST 3: GRACEFUL SHUTDOWN ---");
        int currentLeader = findLeader(nodes);
        if (currentLeader != -1) {
            nodes.get(currentLeader).stop();
        }
        Thread.sleep(5000);
        printStatus(nodes);

        // TEST 4: RANDOM FAILURES AND RECOVERIES
        System.out.println("\n--- TEST 4: RANDOM FAILURES AND RECOVERIES ---");
        simulateRandomFailures(nodes);
        Thread.sleep(10000);
        printStatus(nodes);

        System.out.println("\n=== SIMULATION COMPLETE ===");
        nodes.values().forEach(Node::stop);
        threadPool.shutdownNow();
    }

    private static void printStatus(Map<Integer, Node> nodes) {
        System.out.println("\n=== CLUSTER STATUS ===");
        for (Node node : nodes.values()) {
            String status = node.isActive()
                    ? (node.getId() == node.getCurrentLeaderId() ? "LEADER" : "FOLLOWER")
                    : "DOWN";
            System.out.printf("Node %d: %s (leader: %d)%n",
                    node.getId(), status, node.getCurrentLeaderId());
        }
        System.out.println();
    }

    private static int findLeader(Map<Integer, Node> nodes) {
        for (Node node : nodes.values()) {
            if (node.isActive() && node.getId() == node.getCurrentLeaderId()) {
                return node.getId();
            }
        }
        return -1;
    }

    private static void simulateRandomFailures(Map<Integer, Node> nodes) throws InterruptedException {
        int[] failOrder = {3, 1, 4};
        for (int id : failOrder) {
            Node node = nodes.get(id);
            if (node != null && node.isActive()) {
                System.out.printf("--- Failing node %d ---\n", id);
                node.setStatus(false);
                Thread.sleep(3000);
                System.out.printf("--- Recovering node %d ---\n", id);
                node.setStatus(true);
                Thread.sleep(3000);
            }
        }
    }
}

