package company.vk.edu.distrib.compute.solntseva_nastya;

/**
 * Supported sharding algorithms.
 */
public enum SolnHashiStrategy {
    /** Rendezvous (HRW) hashing. */
    RENDEZVOUS,
    /** Consistent hashing with virtual nodes. */
    CONSISTENT
}
