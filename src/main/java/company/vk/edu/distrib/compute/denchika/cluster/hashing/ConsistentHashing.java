package company.vk.edu.distrib.compute.denchika.cluster.hashing;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

public class ConsistentHashing implements DistributingAlgorithm {

    private static final int VIRTUAL_NODES = 128;

    private final NavigableMap<Long, String> ring = new TreeMap<>();

    public ConsistentHashing(List<String> nodes) {
        for (String node : nodes) {
            for (int i = 0; i < VIRTUAL_NODES; i++) {
                ring.put(hash(node + "#" + i), node);
            }
        }
    }

    @Override
    public String selectNode(String key, List<String> nodes) {
        if (ring.isEmpty()) {
            throw new IllegalStateException("Ring is empty");
        }

        long h = hash(key);

        Map.Entry<Long, String> entry = ring.ceilingEntry(h);

        if (entry == null) {
            entry = ring.firstEntry();
        }

        return entry.getValue();
    }

    private long hash(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] d = md.digest(s.getBytes(StandardCharsets.UTF_8));
            return java.nio.ByteBuffer.wrap(d).getLong();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
