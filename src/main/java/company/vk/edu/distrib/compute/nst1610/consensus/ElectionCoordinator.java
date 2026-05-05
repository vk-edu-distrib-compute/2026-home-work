package company.vk.edu.distrib.compute.nst1610.consensus;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

final class ElectionCoordinator {
    private final AtomicLong nextClock = new AtomicLong(1);
    private final AtomicLong committedClock = new AtomicLong(0);
    private final AtomicInteger committedLeader = new AtomicInteger(-1);
    private final ReentrantLock commitLock = new ReentrantLock();

    public long getNextElectionClock() {
        return nextClock.getAndIncrement();
    }

    public boolean tryCommitVictory(long clock, int leaderId) {
        commitLock.lock();
        try {
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
        } finally {
            commitLock.unlock();
        }
    }

    public long getCommittedClock() {
        return committedClock.get();
    }

    public int getCommittedLeader() {
        return committedLeader.get();
    }
}
