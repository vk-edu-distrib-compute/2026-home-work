package company.vk.edu.distrib.compute.luckyslon2003.consensus;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Cluster {

    private final Map<Integer, Node> nodes;
    private final ExecutorService executor;

    public Cluster(int size) {
        Map<Integer, Node> map = IntStream.rangeClosed(1, size)
                .boxed()
                .collect(Collectors.toMap(i -> i, Node::new, (a, b) -> a, ConcurrentHashMap::new));
        nodes = Collections.unmodifiableMap(map);
        nodes.values().forEach(n -> n.setPeers(nodes));
        executor = Executors.newFixedThreadPool(size, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
        nodes.values().forEach(executor::submit);
    }

    public void startInitialElection() {
        getLogger().info("\n=== Cluster starting: initiating leader election ===\n");
        nodes.values().iterator().next().startElection();
    }

    public Node getNode(int id) {
        return nodes.get(id);
    }

    public Map<Integer, Node> getNodes() {
        return nodes;
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

    public void printStatus() {
        getLogger().info("\n--- Cluster status ---");
        nodes.values().forEach(n ->
                getLogger().info(String.format("  Node %2d  state=%-8s  leader=%s%n",
                        n.getId(),
                        n.getState(),
                        n.getLeaderId() < 0 ? "?" : n.getLeaderId())));
        getLogger().info("----------------------\n");
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

    private java.util.logging.Logger getLogger() {
        return java.util.logging.Logger.getLogger(Cluster.class.getName());
    }
}
