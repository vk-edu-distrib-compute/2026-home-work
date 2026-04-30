package company.vk.edu.distrib.compute.nihuaway00.cluster;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class RendezvousHashingStrategy implements ShardingStrategy {
    private final Map<String, ClusterNode> nodes;
    private static final ThreadLocal<MessageDigest> MD =
            ThreadLocal.withInitial(RendezvousHashingStrategy::createMD5);

    public RendezvousHashingStrategy(Map<String, ClusterNode> nodes) {
        this.nodes = nodes;
    }

    @Override
    public ClusterNode getResponsibleNode(String key) {
        Optional<ClusterNode> targetNode = nodes.values().stream()
                .filter(ClusterNode::isEnabled)
                .map(n -> Map.entry(n, computeHash(key, n.getEndpoint())))
                .max(Comparator.comparingLong(Map.Entry::getValue))
                .map(Map.Entry::getKey);

        if (targetNode.isEmpty()) {
            throw new IllegalStateException("No nodes available");
        }

        return targetNode.get();
    }

    @Override
    public void enableNode(String endpoint) {
        nodes.get(endpoint).enable();
    }

    @Override
    public void disableNode(String endpoint) {
        nodes.get(endpoint).disable();
    }

    @Override
    public List<String> getEndpoints() {
        return nodes.values().stream().map(ClusterNode::getEndpoint).toList();
    }

    private long computeHash(String key, String endpoint) {
        byte[] hash = MD.get().digest((key + "\0" + endpoint).getBytes(StandardCharsets.UTF_8));
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
