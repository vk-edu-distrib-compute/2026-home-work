package company.vk.edu.distrib.compute.maryarta.sharding;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class ConsistentHashing implements ShardingStrategy {
    private static final int VIRTUAL_NODES = 100;
    private final NavigableMap<Long, String> endpointsHash = new TreeMap<>();
    private final Set<String> uniqueEndpoints;

    public ConsistentHashing(List<String> endpoints) {
        this.uniqueEndpoints = Set.copyOf(endpoints);

        for (String endpoint: endpoints) {
            for (int i = 0; i < VIRTUAL_NODES; i++) {
                this.endpointsHash.put(hash(endpoint + '_' + i), endpoint);
            }
        }
    }

    @Override
    public String getEndpoint(String key) {
        return getReplicaEndpoints(key,1).getFirst();
    }

    @Override
    public List<String> getReplicaEndpoints(String key, int replicationFactor) {
        if (replicationFactor > uniqueEndpoints.size()) {
            throw new IllegalArgumentException(
                    "Replication factor " + replicationFactor
                            + " is greater than number of unique endpoints " + uniqueEndpoints.size()
            );
        }
        long keyHash = hash(key);
        LinkedHashSet<String> replicas = new LinkedHashSet<>(replicationFactor);
        Map.Entry<Long, String> start = endpointsHash.ceilingEntry(keyHash);
        Long currentKey = (start != null) ? start.getKey() : endpointsHash.firstKey();
        while (replicas.size() < replicationFactor) {
            String endpoint = endpointsHash.get(currentKey);
            replicas.add(endpoint);
            Long nextKey = endpointsHash.higherKey(currentKey);
            currentKey = (nextKey != null) ? nextKey : endpointsHash.firstKey();
        }
        return List.copyOf(replicas);
    }

    private long hash(String value) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(value.getBytes());
            return ByteBuffer.wrap(digest).getLong();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm is not available", e);
        }
    }

}
