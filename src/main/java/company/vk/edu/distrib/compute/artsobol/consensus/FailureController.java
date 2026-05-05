package company.vk.edu.distrib.compute.artsobol.consensus;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

final class FailureController {
    private static final long NOT_SCHEDULED = 0L;

    private final int nodeId;
    private final ConsensusCluster cluster;
    private final FailurePolicy policy;
    private long nextFailureCheckNanos;
    private long automaticRecoveryNanos;

    FailureController(int nodeId, ConsensusCluster cluster, FailurePolicy policy) {
        this.nodeId = nodeId;
        this.cluster = cluster;
        this.policy = policy;
    }

    void failIfDue(long now) {
        if (!policy.enabled()) {
            return;
        }
        if (nextFailureCheckNanos == NOT_SCHEDULED) {
            nextFailureCheckNanos = now + policy.checkInterval().toNanos();
            return;
        }
        if (now < nextFailureCheckNanos) {
            return;
        }
        nextFailureCheckNanos = now + policy.checkInterval().toNanos();
        if (ThreadLocalRandom.current().nextDouble() < policy.failureProbability()) {
            automaticRecoveryNanos = now + randomRecoveryDelay(policy).toNanos();
            cluster.failNode(nodeId);
        }
    }

    void recoverIfDue(long now) {
        if (automaticRecoveryNanos != NOT_SCHEDULED && now >= automaticRecoveryNanos) {
            cluster.restoreNode(nodeId);
        }
    }

    void resetRecovery() {
        automaticRecoveryNanos = NOT_SCHEDULED;
    }

    private static Duration randomRecoveryDelay(FailurePolicy policy) {
        long minNanos = policy.minRecoveryDelay().toNanos();
        long maxNanos = policy.maxRecoveryDelay().toNanos();
        if (minNanos == maxNanos) {
            return policy.minRecoveryDelay();
        }
        long delay = ThreadLocalRandom.current().nextLong(minNanos, maxNanos + 1L);
        return Duration.ofNanos(delay);
    }
}
