package company.vk.edu.distrib.compute.andeco.election;

import java.util.List;
import java.util.Objects;

public record Cluster(List<ElectionNode> nodes) {
    public Cluster(List<ElectionNode> nodes) {
        Objects.requireNonNull(nodes);
        this.nodes = List.copyOf(nodes);
    }

    public void disableNode(int nodeId) {
        ElectionNode node = nodeById(nodeId);
        if (node == null) {
            return;
        }
        node.forceDown();
    }

    public void enableNode(int nodeId) {
        ElectionNode node = nodeById(nodeId);
        if (node == null) {
            return;
        }
        node.forceUp();
    }

    public void gracefulShutdownLeader() {
        ElectionNode leader = currentLeaderNode();
        if (leader == null) {
            return;
        }
        leader.gracefulShutdown();
    }

    public ElectionNode currentLeaderNode() {
        ElectionNode leader = null;
        for (ElectionNode node : nodes) {
            if (node.role() == NodeRole.LEADER) {
                if (leader != null) {
                    return null;
                }
                leader = node;
            }
        }
        return leader;
    }

    public void send(int toId, Message message) {
        ElectionNode node = nodeById(toId);
        if (node == null) {
            return;
        }
        node.enqueue(message);
    }

    public void broadcast(Message message) {
        for (ElectionNode node : nodes) {
            node.enqueue(message);
        }
    }

    public ElectionNode nodeById(int id) {
        for (ElectionNode node : nodes) {
            if (node.id() == id) {
                return node;
            }
        }
        return null;
    }
}
