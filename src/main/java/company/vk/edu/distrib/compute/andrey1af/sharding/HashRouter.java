package company.vk.edu.distrib.compute.andrey1af.sharding;

@FunctionalInterface
public interface HashRouter {

    String getEndpoint(String key);

    default boolean isResponsible(String key, String endpoint) {
        return getEndpoint(key).equals(endpoint);
    }
}
