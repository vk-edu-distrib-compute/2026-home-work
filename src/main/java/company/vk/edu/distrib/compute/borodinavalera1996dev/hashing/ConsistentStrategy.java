package company.vk.edu.distrib.compute.borodinavalera1996dev.hashing;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import company.vk.edu.distrib.compute.borodinavalera1996dev.cluster.Node;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class ConsistentStrategy implements HashingStrategy {

    private final HashFunction hf = Hashing.murmur3_128();
    private static final int VIRTUAL_NODES = 5;

    private final NavigableMap<Long, Node> circle = new TreeMap<>();
    private final int size;

    public ConsistentStrategy(Collection<Node> nodes) {
        this.size = nodes.size();
        for (Node node : nodes) {
            for (int i = 0; i < VIRTUAL_NODES; i++) {
                addNode(node.name() + i, node);
            }
        }
    }

    private void addNode(String nodeName, Node node) {
        circle.put(hash(nodeName), node);
    }

    public void removeNode(Node node) {
        circle.remove(hash(node.toString()));
    }

    @Override
    public Node getNode(String key) {
        if (circle.isEmpty()) {
            return null;
        }

        long hash = hash(key);
        SortedMap<Long, Node> tailMap = circle.tailMap(hash);

        Long nodeHash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
        return circle.get(nodeHash);
    }

    @Override
    public List<Node> getNodes(String key, int numberOfReplicated) {
        if (circle.isEmpty() || numberOfReplicated <= 0) {
            return Collections.emptyList();
        }
        List<Node> result = new ArrayList<>();
        long hash = hash(key);

        NavigableMap<Long, Node> tailMap = circle.tailMap(hash, true);
        Iterator<Node> iterator = tailMap.values().iterator();

        while (result.size() < numberOfReplicated) {
            if (!iterator.hasNext()) {
                iterator = circle.values().iterator();
            }

            Node node = iterator.next();
            if (!result.contains(node) && node.status()) {
                result.add(node);
            }

            if (result.size() == size) {
                break;
            }
        }

        return result;
    }

    private long hash(String key) {
        HashCode hc = hf.hashString(key, StandardCharsets.UTF_8);
        return hc.asLong();
    }
}
