package company.vk.edu.distrib.compute.artttnik;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Cluster {
    private final Map<Integer, Node> nodes = new ConcurrentHashMap<>();

    public void addNode(Node n) {
        nodes.put(n.getNodeId(), n);
    }

    public Collection<Integer> nodeIds() {
        List<Integer> ids = new ArrayList<>(nodes.keySet());
        Collections.sort(ids);
        return ids;
    }

    public void startAll() {
        nodes.values().forEach(Thread::start);
    }

    public void shutdownAll() {
        nodes.values().forEach(Node::shutdownNode);
    }

    public Node getNode(int id) {
        return nodes.get(id);
    }

    public void send(int fromId, int toId, Message m) {
        Node target = nodes.get(toId);
        if (target == null) {
            return;
        }
        target.receive(m);
    }
}
