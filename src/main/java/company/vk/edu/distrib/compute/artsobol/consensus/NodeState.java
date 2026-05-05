package company.vk.edu.distrib.compute.artsobol.consensus;

public record NodeState(int nodeId, boolean active, NodeRole role, int leaderId) {
}
