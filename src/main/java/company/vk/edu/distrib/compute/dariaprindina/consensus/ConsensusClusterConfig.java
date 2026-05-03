package company.vk.edu.distrib.compute.dariaprindina.consensus;

import java.time.Duration;
import java.util.Objects;

public record ConsensusClusterConfig(
    Duration heartbeatInterval,
    Duration answerTimeout,
    Duration restoreDelay,
    double failureProbabilityPerTick
) {
    public ConsensusClusterConfig {
        Objects.requireNonNull(heartbeatInterval, "heartbeatInterval");
        Objects.requireNonNull(answerTimeout, "answerTimeout");
        Objects.requireNonNull(restoreDelay, "restoreDelay");
        if (heartbeatInterval.isNegative() || heartbeatInterval.isZero()) {
            throw new IllegalArgumentException("heartbeatInterval must be > 0");
        }
        if (answerTimeout.isNegative() || answerTimeout.isZero()) {
            throw new IllegalArgumentException("answerTimeout must be > 0");
        }
        if (restoreDelay.isNegative() || restoreDelay.isZero()) {
            throw new IllegalArgumentException("restoreDelay must be > 0");
        }
        if (failureProbabilityPerTick < 0 || failureProbabilityPerTick > 1) {
            throw new IllegalArgumentException("failureProbabilityPerTick must be in [0, 1]");
        }
    }
}
