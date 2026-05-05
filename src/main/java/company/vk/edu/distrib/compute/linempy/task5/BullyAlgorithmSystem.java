package company.vk.edu.distrib.compute.linempy.task5;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BullyAlgorithmSystem {
    public static void main(String[] args) throws InterruptedException {
        int nodeCount = 5;
        Map<Integer, Node> nodes = new ConcurrentHashMap<>();

        System.out.println("=== INITIALIZING CLUSTER WITH " + nodeCount + " NODES ===");
        System.out.println("Parameters: pingInterval=3s, pingTimeout=5s, electionTimeout=2s\n");

        for (int i = 1; i <= nodeCount; i++) {
            nodes.put(i, new Node(i, nodes));
        }

        ExecutorService threadPool = Executors.newFixedThreadPool(nodeCount);
        nodes.values().forEach(threadPool::submit);

        Thread.sleep(4000);
        printStatus(nodes);

        // TEST 1: LEADER FAILURE (Node 5)
        // Здесь мы увидим время детекции (pingTimeout) + время выборов
        System.out.println("\n--- TEST 1: LEADER FAILURE (Node 5) ---");
        nodes.get(5).setStatus(false);
        Thread.sleep(7000);
        printStatus(nodes);

        // TEST 2: NODE RECOVERY (Node 5)
        // Здесь время выборов будет минимальным, так как Node 5 инициирует их сразу
        System.out.println("\n--- TEST 2: NODE RECOVERY (Node 5) ---");
        nodes.get(5).setStatus(true);
        Thread.sleep(4000);
        printStatus(nodes);

        // TEST 3: GRACEFUL SHUTDOWN
        // Проверка ускоренных выборов через прямую провокацию в методе stop()
        System.out.println("\n--- TEST 3: GRACEFUL SHUTDOWN (Current Leader) ---");
        int currentLeader = findLeader(nodes);
        if (currentLeader != -1) {
            nodes.get(currentLeader).stop();
        }
        Thread.sleep(4000);
        printStatus(nodes);

        // TEST 4: RANDOM FAILURES AND RECOVERIES
        System.out.println("\n--- TEST 4: RANDOM FAILURES AND RECOVERIES ---");
        simulateRandomFailures(nodes);
        Thread.sleep(5000);
        printStatus(nodes);

        System.out.println("\n=== SIMULATION COMPLETE ===");
        nodes.values().stream().filter(Node::isActive).forEach(Node::stop);
        threadPool.shutdownNow();
    }

    private static void printStatus(Map<Integer, Node> nodes) {
        System.out.println("------------------------------------");
        System.out.println("CURRENT CLUSTER STATUS:");
        for (Node node : nodes.values()) {
            String status = node.isActive()
                    ? (node.getId() == node.getCurrentLeaderId() ? "LEADER" : "FOLLOWER")
                    : "DOWN";
            System.out.printf("Node %d: %-10s (leader: %d)%n",
                    node.getId(), status, node.getCurrentLeaderId());
        }
        System.out.println("------------------------------------");
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
        int[] failOrder = {3, 1};
        for (int id : failOrder) {
            Node node = nodes.get(id);
            if (node != null && node.isActive()) {
                System.out.printf(">>> Node %d is failing...\n", id);
                node.setStatus(false);
                Thread.sleep(2000);
                System.out.printf(">>> Node %d is recovering...\n", id);
                node.setStatus(true);
                Thread.sleep(4000);
            }
        }
    }
}
