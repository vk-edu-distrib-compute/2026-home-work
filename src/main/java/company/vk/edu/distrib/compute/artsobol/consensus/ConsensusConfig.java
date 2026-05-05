package company.vk.edu.distrib.compute.artsobol.consensus;

import java.time.Duration;
import java.util.Objects;

public record ConsensusConfig(
        Duration pingInterval,
        Duration pingTimeout,
        Duration electionTimeout,
        FailurePolicy failurePolicy
) {
    public ConsensusConfig {
        Objects.requireNonNull(pingInterval);
        Objects.requireNonNull(pingTimeout);
        Objects.requireNonNull(electionTimeout);
        Objects.requireNonNull(failurePolicy);
        validatePositive(pingInterval, "Ping interval");
        validatePositive(pingTimeout, "Ping timeout");
        validatePositive(electionTimeout, "Election timeout");
    }

    public static ConsensusConfig defaults() {
        return new ConsensusConfig(
                Duration.ofMillis(100L),
                Duration.ofMillis(250L),
                Duration.ofMillis(250L),
                FailurePolicy.disabled()
        );
    }

    private static void validatePositive(Duration duration, String name) {
        if (duration.isNegative() || duration.isZero()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }
}
