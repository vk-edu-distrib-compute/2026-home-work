package company.vk.edu.distrib.compute.wolfram158;

import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class ConsistentHashing implements Router {
    private final NavigableMap<BigInteger, String> ring;
    private final int vnodesPerNode;
    private final HashFunction hashFunction;

    public ConsistentHashing(List<String> endpoints, int vnodesPerNode) throws NoSuchAlgorithmException {
        this.ring = new TreeMap<>();
        this.vnodesPerNode = vnodesPerNode;
        this.hashFunction = new HashFunction();
        for (String endpoint : endpoints) {
            addNode(endpoint);
        }
    }

    private void addNode(String endpoint) throws NoSuchAlgorithmException {
        for (int i = 0; i < vnodesPerNode; i++) {
            final String vnodeKey = endpoint + i;
            final BigInteger hash = hashFunction.getHash(vnodeKey);
            ring.put(hash, endpoint);
        }
    }

    @Override
    public String getNode(String key) throws NoSuchAlgorithmException {
        if (ring.isEmpty()) {
            return null;
        }
        final BigInteger hash = hashFunction.getHash(key);
        Map.Entry<BigInteger, String> entry = ring.ceilingEntry(hash);
        if (entry == null) {
            entry = ring.firstEntry();
        }
        return entry.getValue();
    }
}
