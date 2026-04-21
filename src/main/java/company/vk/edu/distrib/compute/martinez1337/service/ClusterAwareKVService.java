package company.vk.edu.distrib.compute.martinez1337.service;

import java.util.List;

@FunctionalInterface
public interface ClusterAwareKVService {
    void setCluster(List<String> allEndpoints, int myNodeId);
}
