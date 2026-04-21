package company.vk.edu.distrib.compute.maryarta;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class ConsistentHashing implements ShardingStrategy {
    private static final int VIRTUAL_NODES = 100;
    private final NavigableMap<Long, String> endpointsHash = new TreeMap<>();

    public ConsistentHashing(List<String> endpoints) {
        for (String endpoint: endpoints) {
            for (int i = 0; i < VIRTUAL_NODES; i++) {
                this.endpointsHash.put(hash(endpoint + '_' + i), endpoint);
            }
        }
    }

    @Override
    public String getEndpoint(String key) {
        Long keyHash = hash(key);
        Map.Entry<Long, String> entry = endpointsHash.ceilingEntry(keyHash);
        if (entry == null) {
            entry = endpointsHash.firstEntry();
        }
        return entry.getValue();
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
