package company.vk.edu.distrib.compute.wedwincode.sharded;

/**
 * This interface determines what node is responsible for handling certain entity id.
 */
@FunctionalInterface
public interface HashStrategy {
    String getEndpoint(String id);
}
