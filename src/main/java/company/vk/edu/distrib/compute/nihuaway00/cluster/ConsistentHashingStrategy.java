package company.vk.edu.distrib.compute.nihuaway00.cluster;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class ConsistentHashingStrategy implements ShardingStrategy {
    private final NavigableMap<Long, ClusterNode> ring = new TreeMap<>();
    private final int virtualNodes;
    private static final ThreadLocal<MessageDigest> MD =
            ThreadLocal.withInitial(ConsistentHashingStrategy::createMD5);

    public ConsistentHashingStrategy(Map<String, ClusterNode> nodes, int virtualNodes) {
        this.virtualNodes = virtualNodes;
        nodes.values().forEach(this::addNode);
    }

    private void addNode(ClusterNode node) {
        for (int i = 0; i < virtualNodes; i++) {
            long hash = computeHash(node.getEndpoint() + "#" + i);
            ring.put(hash, node);
        }
    }

    @Override
    public ClusterNode getResponsibleNode(String key) {
        long hash = computeHash(key);
        // от ключа до конца
        for (ClusterNode node : ring.tailMap(hash).values()) {
            if (node.isEnabled()) {
                return node;
            }
        }
        // от начала до ключа
        for (ClusterNode node : ring.headMap(hash).values()) {
            if (node.isEnabled()) {
                return node;
            }
        }
        throw new IllegalStateException("No nodes available");
    }

    @Override
    public void enableNode(String endpoint) {
        ring.values().stream()
                .filter(n -> n.getEndpoint().equals(endpoint))
                .forEach(ClusterNode::enable);
    }

    @Override
    public void disableNode(String endpoint) {
        ring.values().stream()
                .filter(n -> n.getEndpoint().equals(endpoint))
                .forEach(ClusterNode::disable);
    }

    @Override
    public List<String> getEndpoints() {
        return ring.values().stream()
                .map(ClusterNode::getEndpoint)
                .distinct()
                .toList();
    }

    private long computeHash(String key) {
        byte[] hash = MD.get().digest(key.getBytes(StandardCharsets.UTF_8));
        return ByteBuffer.wrap(hash).getLong();
    }

    private static MessageDigest createMD5() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError(e);
        }
    }
}
