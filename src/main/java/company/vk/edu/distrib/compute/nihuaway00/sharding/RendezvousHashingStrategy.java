package company.vk.edu.distrib.compute.nihuaway00.sharding;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

public class RendezvousHashingStrategy implements ShardingStrategy {
    private CopyOnWriteArrayList<NodeInfo> nodes;

    public RendezvousHashingStrategy(CopyOnWriteArrayList<NodeInfo> nodes) {
        this.nodes = nodes;
    }

    @Override
    public NodeInfo getResponsibleNode(String key) {
        Optional<NodeInfo> targetNode = nodes.stream()
                .map(n -> Map.entry(n, computeHash(key, n.getEndpoint())))
                .max(Comparator.comparingLong(Map.Entry::getValue))
                .map(Map.Entry::getKey);

        if (targetNode.isEmpty()) {
            throw new IllegalStateException("No nodes available");
        }

        return targetNode.get();
    }

    private long computeHash(String key, String endpoint) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hash = md.digest((key + endpoint).getBytes(StandardCharsets.UTF_8));
            return ByteBuffer.wrap(hash).getLong();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e); // MD5 всегда есть в JVM
        }
    }
}
