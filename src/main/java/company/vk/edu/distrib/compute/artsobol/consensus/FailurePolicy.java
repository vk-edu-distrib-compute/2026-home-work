package company.vk.edu.distrib.compute.artsobol.consensus;

import java.time.Duration;
import java.util.Objects;

public record FailurePolicy(
        double failureProbability,
        Duration checkInterval,
        Duration minRecoveryDelay,
        Duration maxRecoveryDelay
) {
    private static final FailurePolicy DISABLED = new FailurePolicy(
            0.0d,
            Duration.ofSeconds(1L),
            Duration.ofSeconds(1L),
            Duration.ofSeconds(1L)
    );

    public FailurePolicy {
        Objects.requireNonNull(checkInterval);
        Objects.requireNonNull(minRecoveryDelay);
        Objects.requireNonNull(maxRecoveryDelay);
        if (failureProbability < 0.0d || failureProbability > 1.0d) {
            throw new IllegalArgumentException("Failure probability must be in [0, 1]");
        }
        if (checkInterval.isNegative() || checkInterval.isZero()) {
            throw new IllegalArgumentException("Failure check interval must be positive");
        }
        if (minRecoveryDelay.isNegative() || minRecoveryDelay.isZero()) {
            throw new IllegalArgumentException("Min recovery delay must be positive");
        }
        if (maxRecoveryDelay.compareTo(minRecoveryDelay) < 0) {
            throw new IllegalArgumentException("Max recovery delay must not be less than min recovery delay");
        }
    }

    public static FailurePolicy disabled() {
        return DISABLED;
    }

    boolean enabled() {
        return failureProbability > 0.0d;
    }
}
