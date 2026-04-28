package company.vk.edu.distrib.compute.mediocritas.cluster.routing;

import company.vk.edu.distrib.compute.mediocritas.cluster.Node;

import java.util.List;

public interface Router {

    void addNode(Node node);

    void removeNode(Node node);

    Node getNodeForKey(String key);

    List<Node> getAllNodes();
}
