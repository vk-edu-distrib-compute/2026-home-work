package company.vk.edu.distrib.compute.nihuaway00.task5;

public record FailureConfig(
        double probability,
        long diceIntervalMs,
        long minRecoveryDelayMs,
        long maxRecoveryDelayMs
) {
    public FailureConfig {
        if (probability < 0 || probability > 1) {
            throw new IllegalArgumentException("probability must be in range [0, 1]");
        }
        if (diceIntervalMs <= 0) {
            throw new IllegalArgumentException("diceIntervalMs must be > 0");
        }
        if (minRecoveryDelayMs <= 0) {
            throw new IllegalArgumentException("minRecoveryDelayMs must be > 0");
        }
        if (maxRecoveryDelayMs <= 0) {
            throw new IllegalArgumentException("maxRecoveryDelayMs must be > 0");
        }
        if (minRecoveryDelayMs > maxRecoveryDelayMs) {
            throw new IllegalArgumentException("minRecoveryDelayMs must be <= maxRecoveryDelayMs");
        }
    }

    public boolean isEnabled() {
        return probability > 0;
    }

    public static FailureConfig disabled() {
        return new FailureConfig(0, 2_000, 2_000, 5_000);
    }
}
