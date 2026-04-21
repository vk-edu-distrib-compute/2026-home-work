package company.vk.edu.distrib.compute.marinchanka;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class ConsistentHashingRouter {
    private final int virtualNodesCount;
    private final SortedMap<Long, ClusterNode> ring = new ConcurrentSkipListMap<>();
    private final List<ClusterNode> nodes = new ArrayList<>();

    public ConsistentHashingRouter(int virtualNodesCount) {
        this.virtualNodesCount = virtualNodesCount;
    }

    public void addNode(ClusterNode node) {
        nodes.add(node);
        for (int i = 0; i < virtualNodesCount; i++) {
            long hash = hash(node.getEndpoint() + "-" + i);
            ring.put(hash, node);
        }
    }

    public void removeNode(ClusterNode node) {
        nodes.remove(node);
        for (int i = 0; i < virtualNodesCount; i++) {
            long hash = hash(node.getEndpoint() + "-" + i);
            ring.remove(hash);
        }
    }

    public ClusterNode getNode(String key) {
        if (ring.isEmpty()) {
            throw new IllegalStateException("No nodes in cluster");
        }

        long hash = hash(key);
        SortedMap<Long, ClusterNode> tailMap = ring.tailMap(hash);
        Long nodeHash = tailMap.isEmpty() ? ring.firstKey() : tailMap.firstKey();

        return ring.get(nodeHash);
    }

    private long hash(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(input.getBytes());
            byte[] digest = md.digest();

            long hash = 0;
            for (int i = 0; i < 8 && i < digest.length; i++) {
                hash = (hash << 8) | (digest[i] & 0xFF);
            }
            return Math.abs(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not available", e);
        }
    }

    public List<ClusterNode> getNodes() {
        return Collections.unmodifiableList(nodes);
    }
}
