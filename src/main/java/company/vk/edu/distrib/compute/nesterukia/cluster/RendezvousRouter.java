package company.vk.edu.distrib.compute.nesterukia.cluster;

import company.vk.edu.distrib.compute.nesterukia.KVServiceImpl;

import java.util.Map;

public class RendezvousRouter implements Router {
    private Map<String, KVServiceImpl> nodesMap;

    @Override
    public KVServiceImpl getNode(String key) {
        if (nodesMap == null || nodesMap.isEmpty()) {
            return null;
        }

        String selectedNode = null;
        int maxHash = -1;

        for (String nodeName : nodesMap.keySet()) {
            String combined = key + nodeName;
            int hash = combined.hashCode();

            if (hash > maxHash) {
                maxHash = hash;
                selectedNode = nodeName;
            }
        }

        return nodesMap.get(selectedNode);
    }

    @Override
    public void setNodesMap(Map<String, KVServiceImpl> nodesMap) {
        this.nodesMap = nodesMap;
    }
}
