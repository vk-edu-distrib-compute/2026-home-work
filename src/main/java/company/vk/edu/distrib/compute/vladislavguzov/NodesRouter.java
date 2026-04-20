package company.vk.edu.distrib.compute.vladislavguzov;

import java.security.NoSuchAlgorithmException;

public interface NodesRouter {

    void add(ClusterNode node, String id);

    void remove(String id);

    ClusterNode get(String key) throws NoSuchAlgorithmException;
}
