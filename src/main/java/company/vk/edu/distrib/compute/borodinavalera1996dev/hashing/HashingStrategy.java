package company.vk.edu.distrib.compute.borodinavalera1996dev.hashing;

import company.vk.edu.distrib.compute.borodinavalera1996dev.cluster.Node;

import java.util.List;

public interface HashingStrategy {

    Node getNode(String key);

    List<Node> getNodes(String key, int numberOfReplicated);

}
