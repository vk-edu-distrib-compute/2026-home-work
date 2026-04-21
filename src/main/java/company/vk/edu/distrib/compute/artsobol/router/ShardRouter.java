package company.vk.edu.distrib.compute.artsobol.router;

@FunctionalInterface
public interface ShardRouter {
    String getOwnerEndpoint(String key);
}
