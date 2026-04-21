package company.vk.edu.distrib.compute.v11qfour.cluster;

import java.util.List;

@FunctionalInterface
public interface V11qfourRoutingStrategy {
    V11qfourNode getResponsibleNode(String key, List<V11qfourNode> allNodes);
}
