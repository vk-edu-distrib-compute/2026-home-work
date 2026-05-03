package company.vk.edu.distrib.compute.dariaprindina.consensus;

public record ConsensusNodeSnapshot(
    int nodeId,
    ConsensusNodeState state,
    Integer knownLeaderId
) {
}
