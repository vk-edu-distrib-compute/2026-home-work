package company.vk.edu.distrib.compute.luckyslon2003.consensus;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Manages the lifecycle of all {@link Node}s in the cluster.
 *
 * <p>Responsibilities:
 * <ul>
 *   <li>Creates N nodes with IDs 1..N.</li>
 *   <li>Injects the full peer map into every node so they can communicate.</li>
 *   <li>Starts each node on a dedicated daemon thread.</li>
 *   <li>Triggers the initial election.</li>
 *   <li>Provides helper methods used by tests / the demo runner.</li>
 * </ul>
 */
public class Cluster {

    private final Map<Integer, Node> nodes;
    private final ExecutorService executor;

    /**
     * Creates a cluster with {@code size} nodes and starts them immediately.
     *
     * @param size number of nodes (IDs will be 1 .. size)
     */
    public Cluster(int size) {
        Map<Integer, Node> map = new LinkedHashMap<>();
        for (int i = 1; i <= size; i++) {
            map.put(i, new Node(i));
        }
        nodes = Collections.unmodifiableMap(map);

        // Wire every node with the full topology
        nodes.values().forEach(n -> n.setPeers(nodes));

        // Launch each node on its own thread
        executor = Executors.newFixedThreadPool(size, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
        nodes.values().forEach(executor::submit);
    }

    /**
     * Triggers the initial election from the node with the lowest ID.
     * In the Bully algorithm the lowest-ID node starts the election;
     * the highest available node will ultimately win.
     */
    public void startInitialElection() {
        System.out.println("\n=== Cluster starting: initiating leader election ===\n");
        // Any node can start; using the lowest for determinism
        nodes.values().iterator().next().startElection();
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    public Node getNode(int id) {
        return nodes.get(id);
    }

    public Map<Integer, Node> getNodes() {
        return nodes;
    }

    // -----------------------------------------------------------------------
    // Shutdown
    // -----------------------------------------------------------------------

    /**
     * Stops all nodes and the executor service.
     */
    public void shutdown() {
        nodes.values().forEach(Node::stop);
        executor.shutdownNow();
        try {
            executor.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // -----------------------------------------------------------------------
    // Diagnostic helpers
    // -----------------------------------------------------------------------

    /** Prints a one-line summary of every node's current state. */
    public void printStatus() {
        System.out.println("\n--- Cluster status ---");
        nodes.values().forEach(n ->
                System.out.printf("  Node %2d  state=%-8s  leader=%s%n",
                        n.getId(),
                        n.getState(),
                        n.getLeaderId() < 0 ? "?" : n.getLeaderId()));
        System.out.println("----------------------\n");
    }

    /**
     * Returns the ID of the unique leader as agreed upon by all live nodes,
     * or -1 if no consensus exists (or more than one leader is detected).
     */
    public int getConsensusLeader() {
        int consensusLeader = -1;
        for (Node n : nodes.values()) {
            if (n.getState() == NodeState.DOWN) continue;
            if (n.getState() == NodeState.LEADER) {
                if (consensusLeader >= 0 && consensusLeader != n.getId()) {
                    // split-brain detected
                    return -2;
                }
                consensusLeader = n.getId();
            }
        }
        return consensusLeader;
    }
}
