package company.vk.edu.distrib.compute.nst1610.consensus;

public enum ElectionReason {
    RECOVERY,
    START,
    LEADER_MISSING,
    LEADER_TIMEOUT,
    LEADER_UNAVAILABLE,
    ELDER_NODE_ELECT
}
