package company.vk.edu.distrib.compute.aldor7705.hw5;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class Cluster {
    private static final Logger LOGGER = Logger.getLogger(Cluster.class.getName());
    private final Map<Integer, Node> nodes;
    private final ExecutorService executor;

    public Cluster(int size) {
        Map<Integer, Node> map = new ConcurrentHashMap<>();
        for (int i = 1; i <= size; i++) {
            map.put(i, new Node(i));
        }

        for (Node node : map.values()) {
            node.setPeers(map);
        }

        this.nodes = map;
        this.executor = Executors.newFixedThreadPool(size, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        nodes.values().forEach(executor::submit);
    }

    public void startInitialElection() {
        Node initiator = nodes.values().stream()
                .filter(n -> n.getState() == NodeState.FOLLOWER)
                .max(Comparator.comparingInt(Node::getId))
                .orElseThrow();

        initiator.startElection();
    }

    public Node getNode(int id) {
        return nodes.get(id);
    }

    public Map<Integer, Node> getNodes() {
        return nodes;
    }

    public int getConsensusLeader() {
        int consensusLeader = -1;
        for (Node n : nodes.values()) {
            if (n.getState() == NodeState.DOWN) {
                continue;
            }
            if (n.getState() == NodeState.LEADER) {
                if (consensusLeader >= 0 && consensusLeader != n.getId()) {
                    return -2;
                }
                consensusLeader = n.getId();
            }
        }
        return consensusLeader;
    }

    public void printStatus() {
        LOGGER.info("\n=== СОСТОЯНИЕ КЛАСТЕРА ===");
        for (Node n : nodes.values()) {
            LOGGER.info(String.format("  Узел %2d  состояние=%-9s  лидер=%s",
                    n.getId(),
                    n.getState(),
                    n.getLeaderId() < 0 ? "?" : n.getLeaderId()));
        }

        int leader = getConsensusLeader();
        if (leader == -2) {
            LOGGER.info("  *** ОБНАРУЖЕН SPLIT-BRAIN! ***");
        } else if (leader > 0) {
            LOGGER.info("  Текущий лидер: " + leader);
        } else {
            LOGGER.info("  Лидер пока не выбран");
        }
        LOGGER.info("==============================\n");
    }

    public void shutdown() {
        nodes.values().forEach(Node::stop);
        executor.shutdownNow();
        try {
            executor.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
