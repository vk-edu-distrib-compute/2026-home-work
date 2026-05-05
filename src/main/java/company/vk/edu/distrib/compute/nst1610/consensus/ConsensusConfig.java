package company.vk.edu.distrib.compute.nst1610.consensus;

public record ConsensusConfig(
    long loopDelayMillis,
    long pingIntervalMillis,
    long pingTimeoutMillis,
    long electionTimeoutMillis,
    long failureCheckIntervalMillis,
    double failureProbability,
    long recoveryDelayMinMillis,
    long recoveryDelayMaxMillis
) {
    public static ConsensusConfig defaultConfig() {
        return new ConsensusConfig(
            50L,
            250L,
            600L,
            350L,
            500L,
            0.0,
            1_000L,
            2_000L
        );
    }

    public static ConsensusConfig unstableConfig() {
        return new ConsensusConfig(
            50L,
            200L,
            450L,
            250L,
            250L,
            0.18,
            300L,
            900L
        );
    }
}
