package company.vk.edu.distrib.compute.andeco.election;

import java.util.concurrent.TimeUnit;

public record ElectionConfig(long loopPollMs, long pingPeriodMs, long pingTimeoutMs,
                             long answerTimeoutMs, long victoryTimeoutMs, long stepDownCooldownMs) {
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


    public long getPingPeriodNs() {
        return TimeUnit.MILLISECONDS.toNanos(pingPeriodMs);
    }

    public long getPingTimeoutNs() {
        return TimeUnit.MILLISECONDS.toNanos(pingTimeoutMs);
    }

    public long getAnswerTimeoutNs() {
        return TimeUnit.MILLISECONDS.toNanos(answerTimeoutMs);
    }

    public long getVictoryTimeoutNs() {
        return TimeUnit.MILLISECONDS.toNanos(victoryTimeoutMs);
    }

    public long getStepDownCooldownNs() {
        return TimeUnit.MILLISECONDS.toNanos(stepDownCooldownMs);
    }
}

