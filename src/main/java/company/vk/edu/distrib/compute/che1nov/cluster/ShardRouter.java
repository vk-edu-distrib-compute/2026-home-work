package company.vk.edu.distrib.compute.che1nov.cluster;

public interface ShardRouter {
    String endpointByKey(String key);

    java.util.List<String> endpointsByKey(String key, int count);
}
