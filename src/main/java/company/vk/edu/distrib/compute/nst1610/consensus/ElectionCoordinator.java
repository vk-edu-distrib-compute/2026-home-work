package company.vk.edu.distrib.compute.nst1610.consensus;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

final class ElectionCoordinator {
    private final AtomicLong nextClock = new AtomicLong(1);
    private final AtomicLong committedClock = new AtomicLong(0);
    private final AtomicInteger committedLeader = new AtomicInteger(-1);

    public long getNextElectionClock() {
        return nextClock.getAndIncrement();
    }

    public boolean tryCommitVictory(long clock, int leaderId) {
        synchronized (this) {
            long currentEpoch = committedClock.get();
            if (clock < currentEpoch) {
                return false;
            }
            if (clock == currentEpoch) {
                return committedLeader.compareAndSet(-1, leaderId) || committedLeader.get() == leaderId;
            }
            committedClock.set(clock);
            committedLeader.set(leaderId);
            return true;
        }
    }

    public long getCommittedClock() {
        return committedClock.get();
    }

    public int getCommittedLeader() {
        return committedLeader.get();
    }
}
