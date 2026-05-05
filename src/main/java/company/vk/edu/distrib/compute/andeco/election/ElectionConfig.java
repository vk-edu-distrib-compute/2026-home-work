package company.vk.edu.distrib.compute.andeco.election;

import java.util.concurrent.TimeUnit;

public final class ElectionConfig {
    private final long loopPollMs;
    private final long pingPeriodMs;
    private final long pingTimeoutMs;
    private final long answerTimeoutMs;
    private final long victoryTimeoutMs;
    private final long stepDownCooldownMs;

    public ElectionConfig(long loopPollMs,
                          long pingPeriodMs,
                          long pingTimeoutMs,
                          long answerTimeoutMs,
                          long victoryTimeoutMs,
                          long stepDownCooldownMs) {
        this.loopPollMs = loopPollMs;
        this.pingPeriodMs = pingPeriodMs;
        this.pingTimeoutMs = pingTimeoutMs;
        this.answerTimeoutMs = answerTimeoutMs;
        this.victoryTimeoutMs = victoryTimeoutMs;
        this.stepDownCooldownMs = stepDownCooldownMs;
    }

    public static ElectionConfig defaults(long pingTimeoutMs, long answerTimeoutMs, long victoryTimeoutMs) {
        return new ElectionConfig(
                100,
                600,
                pingTimeoutMs,
                answerTimeoutMs,
                victoryTimeoutMs,
                900
        );
    }

    public long loopPollMs() {
        return loopPollMs;
    }

    public long pingPeriodNs() {
        return TimeUnit.MILLISECONDS.toNanos(pingPeriodMs);
    }

    public long pingTimeoutNs() {
        return TimeUnit.MILLISECONDS.toNanos(pingTimeoutMs);
    }

    public long answerTimeoutNs() {
        return TimeUnit.MILLISECONDS.toNanos(answerTimeoutMs);
    }

    public long victoryTimeoutNs() {
        return TimeUnit.MILLISECONDS.toNanos(victoryTimeoutMs);
    }

    public long stepDownCooldownNs() {
        return TimeUnit.MILLISECONDS.toNanos(stepDownCooldownMs);
    }
}

