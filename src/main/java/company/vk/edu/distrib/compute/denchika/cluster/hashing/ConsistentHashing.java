package company.vk.edu.distrib.compute.denchika.cluster.hashing;

import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

public class ConsistentHashing implements DistributingAlgorithm {
    private final NavigableMap<Integer, String> ring = new TreeMap<>();

    public ConsistentHashing(List<String> nodes) {
        for (String node : nodes) {
            ring.put(hash(node), node);
        }
    }

    @Override
    public String selectNode(String key) {
        int keyHash = hash(key);
        var entry = ring.ceilingEntry(keyHash);
        if (entry == null) {
            entry = ring.firstEntry();
        }
        return entry.getValue();
    }

    private int hash(String s) {
        return s.hashCode();
    }
}
