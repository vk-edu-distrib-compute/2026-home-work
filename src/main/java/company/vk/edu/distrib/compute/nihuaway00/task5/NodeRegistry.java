package company.vk.edu.distrib.compute.nihuaway00.task5;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class NodeRegistry {
    private final ConcurrentHashMap<Integer, Node> nodeById = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Thread> threadById = new ConcurrentHashMap<>();

    public Node getNode(int id) {
        return nodeById.get(id);
    }

    public void runAll() {
        threadById.values().forEach(Thread::start);
    }

    public void stopAll() {
        nodeById.values().forEach(Node::shutdown);
        threadById.values().forEach(Thread::interrupt);
        threadById.values().forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException expected) {
                Thread.currentThread().interrupt();
            }
        });
    }

    public void generateNodes(int n) {
        for (int nodeId = 0; nodeId < n; nodeId++) {
            Node node = new Node(this, nodeId);
            nodeById.put(nodeId, node);
            Thread thread = new Thread(node, "task5-node-" + nodeId);
            threadById.put(nodeId, thread);
        }
    }

    public void sendTo(int nodeId, Message message) {
        Node node = nodeById.get(nodeId);
        if (node != null) {
            node.enqueue(message);
        }
    }

    public void broadcast(Message message, int senderNodeId) {
        for (var entry : nodeById.entrySet()) {
            if (entry.getKey() != senderNodeId) {
                entry.getValue().enqueue(message);
            }
        }
    }

    public List<Node> getNodesWithHigherId(int nodeId) {
        return nodeById
                .values()
                .stream()
                .filter(node -> node.getId() > nodeId)
                .sorted(Comparator.comparingInt(Node::getId))
                .toList();
    }

    public List<Node> getAllNodes() {
        return nodeById
                .values()
                .stream()
                .sorted(Comparator.comparingInt(Node::getId))
                .toList();
    }

    public List<Integer> getLeaderIds() {
        return nodeById
                .values()
                .stream()
                .filter(Node::isAlive)
                .filter(node -> node.getState() == NodeState.LEADER)
                .map(Node::getId)
                .sorted()
                .toList();
    }
}
