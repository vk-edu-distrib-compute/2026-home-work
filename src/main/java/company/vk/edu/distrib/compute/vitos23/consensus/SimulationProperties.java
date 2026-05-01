package company.vk.edu.distrib.compute.vitos23.consensus;

import java.time.Duration;

public final class SimulationProperties {

    public static final Duration REQUEST_TIMEOUT = Duration.ofMillis(100);
    public static final Duration PING_INTERVAL = Duration.ofMillis(500);
    public static final Duration ELECTION_TIMEOUT = Duration.ofMillis(300);
    public static final Duration STATE_LOG_INTERVAL = Duration.ofMillis(500);
    public static final Duration FAILURE_INTERVAL = Duration.ofSeconds(2);
    public static final double FAILURE_PROBABILITY = 0.5;
    public static final int NODE_COUNT = 5;

    private SimulationProperties() {
    }
}
