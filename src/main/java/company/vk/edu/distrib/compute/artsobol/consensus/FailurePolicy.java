package company.vk.edu.distrib.compute.artsobol.consensus;

import java.time.Duration;
import java.util.Objects;

public record FailurePolicy(
        double failureProbability,
        Duration checkInterval,
        Duration minRecoveryDelay,
        Duration maxRecoveryDelay
) {
    private static final double MIN_PROBABILITY = 0.0d;
    private static final double MAX_PROBABILITY = 1.0d;
    private static final int SAME_DELAY = 0;
    private static final FailurePolicy DISABLED = new FailurePolicy(
            MIN_PROBABILITY,
            Duration.ofSeconds(1L),
            Duration.ofSeconds(1L),
            Duration.ofSeconds(1L)
    );

    public FailurePolicy {
        Objects.requireNonNull(checkInterval);
        Objects.requireNonNull(minRecoveryDelay);
        Objects.requireNonNull(maxRecoveryDelay);
        if (failureProbability < MIN_PROBABILITY || failureProbability > MAX_PROBABILITY) {
            throw new IllegalArgumentException("Failure probability must be in [0, 1]");
        }
        if (checkInterval.isNegative() || checkInterval.isZero()) {
            throw new IllegalArgumentException("Failure check interval must be positive");
        }
        if (minRecoveryDelay.isNegative() || minRecoveryDelay.isZero()) {
            throw new IllegalArgumentException("Min recovery delay must be positive");
        }
        if (maxRecoveryDelay.compareTo(minRecoveryDelay) < SAME_DELAY) {
            throw new IllegalArgumentException("Max recovery delay must not be less than min recovery delay");
        }
    }

    public static FailurePolicy disabled() {
        return DISABLED;
    }

    boolean enabled() {
        return failureProbability > MIN_PROBABILITY;
    }
}
