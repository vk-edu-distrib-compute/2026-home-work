package company.vk.edu.distrib.compute.nesterukia.cluster;

import company.vk.edu.distrib.compute.nesterukia.KVServiceImpl;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class ConsistentRouter implements Router {
    private Map<String, KVServiceImpl> nodesMap;
    private NavigableMap<Integer, String> hashRing;

    @Override
    public KVServiceImpl getNode(String key) {
        if (nodesMap == null || nodesMap.isEmpty()) {
            return null;
        }

        if (hashRing == null || hashRing.isEmpty()) {
            return null;
        }

        int keyHash = (key + "key").hashCode();

        Map.Entry<Integer, String> entry = hashRing.ceilingEntry(keyHash);

        if (entry == null) {
            entry = hashRing.firstEntry();
        }

        String nodeName = entry.getValue();
        return nodesMap.get(nodeName);
    }

    @Override
    public void setNodesMap(Map<String, KVServiceImpl> nodesMap) {
        this.nodesMap = nodesMap;
        buildHashRing();
    }

    private void buildHashRing() {
        hashRing = new TreeMap<>();

        for (String nodeName : nodesMap.keySet()) {
            addNodeToRing(nodeName);
        }
    }

    private void addNodeToRing(String nodeName) {
        for (int i = 0; i < 3; i++) {
            String virtualNodeName = nodeName + "#" + i;
            int hash = virtualNodeName.hashCode();
            hashRing.put(hash, nodeName);
        }
    }
}
