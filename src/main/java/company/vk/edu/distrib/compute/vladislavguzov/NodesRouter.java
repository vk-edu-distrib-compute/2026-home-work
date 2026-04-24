package company.vk.edu.distrib.compute.vladislavguzov;

import java.security.NoSuchAlgorithmException;

public interface NodesRouter {

    void add(ClusterNode node, String id) throws NoSuchAlgorithmException;

    void remove(String id) throws NoSuchAlgorithmException;

    ClusterNode get(String key) throws NoSuchAlgorithmException;
}
