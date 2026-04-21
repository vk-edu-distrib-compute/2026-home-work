package company.vk.edu.distrib.compute.martinez1337.sharding;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

public class ConsistentHashingSharding implements ShardingStrategy {
    private static final int VIRTUAL_NODES_PER_ENDPOINT = 16;

    @Override
    public int getResponsibleNode(String key, List<String> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("Endpoints cannot be null or empty");
        }

        NavigableMap<Long, Integer> ring = buildRing(endpoints);
        long keyHash = hash(key);

        var entry = ring.ceilingEntry(keyHash);
        if (entry == null) {
            entry = ring.firstEntry();
        }
        return entry.getValue();
    }

    @Override
    public long hash(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            return ((long) (hash[0] & 0xff) << 56)
                    | ((long) (hash[1] & 0xff) << 48)
                    | ((long) (hash[2] & 0xff) << 40)
                    | ((long) (hash[3] & 0xff) << 32)
                    | ((long) (hash[4] & 0xff) << 24)
                    | ((long) (hash[5] & 0xff) << 16)
                    | ((long) (hash[6] & 0xff) << 8)
                    | (hash[7] & 0xff);
        } catch (Exception e) {
            return input.hashCode();
        }
    }

    private NavigableMap<Long, Integer> buildRing(List<String> endpoints) {
        NavigableMap<Long, Integer> ring = new TreeMap<>();
        for (int endIdx = 0; endIdx < endpoints.size(); endIdx++) {
            String endpoint = endpoints.get(endIdx);
            for (int vnode = 0; vnode < VIRTUAL_NODES_PER_ENDPOINT; vnode++) {
                long vnodeHash = hash(endpoint + "#vn" + vnode);
                ring.put(vnodeHash, endIdx);
            }
        }
        return ring;
    }
}
