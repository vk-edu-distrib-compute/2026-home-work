package company.vk.edu.distrib.compute.ronshinvsevolod;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.zip.CRC32;

public class ConsistentHashStrategy implements HashStrategy {
    private final SortedMap<Long, String> circle = new TreeMap<>();

    public ConsistentHashStrategy(List<String> endpoints, int virtualNodes) {
        for (String endpoint : endpoints) {
            for (int i = 0; i < virtualNodes; i++) {
                long hash = hash(endpoint + "#" + i);
                circle.put(hash, endpoint);
            }
        }
    }

    @Override
    public String getEndpoint(String key) {
        if (circle.isEmpty()) {
            return null;
        }
        long hash = hash(key);
        SortedMap<Long, String> tail = circle.tailMap(hash);
        Long nodeHash = tail.isEmpty() ? circle.firstKey() : tail.firstKey();
        return circle.get(nodeHash);
    }

    private long hash(String s) {
        CRC32 crc = new CRC32();
        crc.update(s.getBytes(StandardCharsets.UTF_8));
        return crc.getValue();
    }
}
