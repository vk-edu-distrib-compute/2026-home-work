package company.vk.edu.distrib.compute.solntseva_nastya;

/**
 * Routes a key to a responsible node endpoint.
 */
@FunctionalInterface
public interface Router {

    /**
     * Returns the node endpoint responsible for the given key.
     *
     * @param key the lookup key
     * @return the responsible node URL
     */
    String getNode(String key);
}
