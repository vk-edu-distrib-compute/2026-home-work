package company.vk.edu.distrib.compute.ronshinvsevolod;

@FunctionalInterface
public interface HashStrategy {
    String getEndpoint(String key);
}
