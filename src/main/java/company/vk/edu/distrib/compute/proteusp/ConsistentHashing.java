package company.vk.edu.distrib.compute.proteusp;

import java.util.List;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHashing implements ShardingAlgorithm {
    private static int virtualNodesCount = 100;

    @Override
    public String getNode(String key, List<String> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            return null;
        }

        NavigableMap<Integer, String> circle = new TreeMap<>();

        for (String endpoint : endpoints) {
            for (int i = 0; i < virtualNodesCount; i++) {
                String virtualNode = endpoint + "#" + i;
                int hash = Math.abs(virtualNode.hashCode());
                circle.put(hash, endpoint);
            }
        }

        int keyHash = Math.abs(key.hashCode());

        SortedMap<Integer, String> tailMap = circle.tailMap(keyHash);
        int nodeHash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();

        return circle.get(nodeHash);
    }
}
