package company.vk.edu.distrib.compute.nst1610.consensus;

public record NodeSnapshot(
    int nodeId,
    boolean failed,
    boolean leader,
    int knownLeaderId,
    long leaderClock
) {
}
