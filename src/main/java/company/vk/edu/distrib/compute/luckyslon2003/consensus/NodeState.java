package company.vk.edu.distrib.compute.luckyslon2003.consensus;

/**
 * Possible lifecycle states of a cluster node.
 */
public enum NodeState {
    /** The node is operational and acting as a follower (or newly started). */
    FOLLOWER,
    /** The node is operational and acting as the cluster leader. */
    LEADER,
    /** The node is participating in an ongoing election. */
    ELECTION,
    /** The node has failed and does not process messages. */
    DOWN
}
