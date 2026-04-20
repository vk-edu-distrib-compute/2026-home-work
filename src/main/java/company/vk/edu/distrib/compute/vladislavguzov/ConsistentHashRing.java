package company.vk.edu.distrib.compute.vladislavguzov;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.NavigableMap;
import java.util.TreeMap;

public class ConsistentHashRing implements NodesRouter {
    private final NavigableMap<Long, ClusterNode> ring = new TreeMap<>();
    private final int virtualNodes;

    public ConsistentHashRing(int virtualNodes) {
        this.virtualNodes = virtualNodes > 0 ? virtualNodes : 100;
    }

    private long hash(String key) {
        try {
            byte[] digest = MessageDigest.getInstance("MD5")
                    .digest(key.getBytes(StandardCharsets.UTF_8));
            long h = 0;
            for (int i = 0; i < 8; i++) {
                h |= ((long) (digest[i] & 0xFF)) << (i * 8);
            }
            return h;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Unsupported algorithm", e);
        }
    }

    @Override
    public void add(ClusterNode node, String id) {
        for (int i = 0; i < virtualNodes; i++) {
            ring.put(hash(id + "#" + i), node);
        }
    }

    @Override
    public void remove(String id) {
        for (int i = 0; i < virtualNodes; i++) {
            ring.remove(hash(id + "#" + i));
        }
    }

    @Override
    public ClusterNode get(String key) {
        if (ring.isEmpty()) {
            return null;
        }
        long h = hash(key);
        var entry = ring.ceilingEntry(h);
        return entry != null ? entry.getValue() : ring.firstEntry().getValue();
    }
}
