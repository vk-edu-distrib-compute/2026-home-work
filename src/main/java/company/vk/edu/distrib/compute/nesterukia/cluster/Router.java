package company.vk.edu.distrib.compute.nesterukia.cluster;

import company.vk.edu.distrib.compute.nesterukia.KVServiceImpl;
import company.vk.edu.distrib.compute.nesterukia.utils.HashingAlgorithm;

import java.util.Map;

public interface Router {

    static Router getRouterImpl(HashingAlgorithm hashingAlgorithm) {
        return switch (hashingAlgorithm) {
            case CONSISTENT -> new ConsistentRouter();
            case RENDEZVOUS -> new RendezvousRouter();
        };
    }

    KVServiceImpl getNode(String key);

    void setNodesMap(Map<String, KVServiceImpl> nodesMap);
}
