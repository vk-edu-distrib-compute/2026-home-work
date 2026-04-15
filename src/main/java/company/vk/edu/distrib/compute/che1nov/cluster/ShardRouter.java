package company.vk.edu.distrib.compute.che1nov.cluster;

@FunctionalInterface
public interface ShardRouter {
    String endpointByKey(String key);
}
