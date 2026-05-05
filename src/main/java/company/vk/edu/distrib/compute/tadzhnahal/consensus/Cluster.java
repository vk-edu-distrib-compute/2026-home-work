package company.vk.edu.distrib.compute.tadzhnahal.consensus;

import java.lang.System.Logger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Cluster {
    private static final Logger LOG = System.getLogger(Cluster.class.getName());

    private final List<Node> nodes = new ArrayList<>();

    public Cluster(int nodeCount) {
        if (nodeCount <= 0) {
            throw new IllegalArgumentException("node count must be positive");
        }

        for (int i = 1; i <= nodeCount; i++) {
            nodes.add(new Node(i));
        }
    }

    public List<Node> getNodes() {
        return Collections.unmodifiableList(nodes);
    }

    public void printState() {
        LOG.log(Logger.Level.INFO, "cluster state:");

        for (Node node : nodes) {
            LOG.log(
                    Logger.Level.INFO,
                    "node " + node.getNodeId()
                            + " | " + node.getNodeStatus()
                            + " | inbox=" + node.getInboxSize()
            );
        }
    }
}
