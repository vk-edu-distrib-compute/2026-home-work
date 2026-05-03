package company.vk.edu.distrib.compute.marinchanka;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class RendezvousHashingRouter implements ShardingRouter {
    private final Map<String, ClusterNode> nodes = new ConcurrentHashMap<>();

    @Override
    public void addNode(ClusterNode node) {
        nodes.put(node.getEndpoint(), node);
    }

    @Override
    public void removeNode(ClusterNode node) {
        nodes.remove(node.getEndpoint());
    }

    @Override
    public ClusterNode getNode(String key) {
        if (nodes.isEmpty()) {
            throw new IllegalStateException("No nodes in cluster");
        }

        ClusterNode selected = null;
        long maxWeight = Long.MIN_VALUE;

        for (ClusterNode node : nodes.values()) {
            long weight = hash(key + node.getEndpoint());
            if (weight > maxWeight) {
                maxWeight = weight;
                selected = node;
            }
        }

        return selected;
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
            throw new IllegalStateException("MD5 not available", e);
        }
    }

    public List<ClusterNode> getNodes() {
        return new ArrayList<>(nodes.values());
    }
}
