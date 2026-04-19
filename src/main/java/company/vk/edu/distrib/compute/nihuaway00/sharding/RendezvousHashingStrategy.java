package company.vk.edu.distrib.compute.nihuaway00.sharding;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class RendezvousHashingStrategy implements ShardingStrategy {
    private final Map<String, NodeInfo> nodes;
    private static final ThreadLocal<MessageDigest> MD =
            ThreadLocal.withInitial(RendezvousHashingStrategy::createMD5);

    public RendezvousHashingStrategy(Map<String, NodeInfo> nodes) {
        this.nodes = nodes;
    }

    @Override
    public NodeInfo getResponsibleNode(String key) {
        Optional<NodeInfo> targetNode = nodes.values().stream()
                .filter(NodeInfo::isEnabled)
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
        return nodes.values().stream().map(NodeInfo::getEndpoint).toList();
    }

    private long computeHash(String key, String endpoint) {
        byte[] hash = MD.get().digest((key + endpoint).getBytes(StandardCharsets.UTF_8));
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
