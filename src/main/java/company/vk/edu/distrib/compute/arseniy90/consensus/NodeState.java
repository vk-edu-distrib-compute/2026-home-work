package company.vk.edu.distrib.compute.arseniy90.consensus;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class NodeState {
    private final int id;
    private final AtomicInteger leaderId;
    private final AtomicBoolean isElectionInProgress;
    private final AtomicReference<NodeRole> role;
    private final AtomicLong lastLeaderContactTime = new AtomicLong(0L);

    public NodeState(int id) {
        this.id = id;
        this.leaderId = new AtomicInteger(-1);
        this.isElectionInProgress = new AtomicBoolean(false);
        this.role = new AtomicReference<>(NodeRole.DOWN);
    }

    public int getId() {
        return id;
    }

    public int getLeaderId() {
        return leaderId.get();
    }

    public void setLeaderId(int leaderId) {
        this.leaderId.set(leaderId);
    }

    public NodeRole getRole() {
        return role.get();
    }

    public void setRole(NodeRole role) {
        this.role.set(role);
    }

    public boolean compareAndSetElection(boolean expect, boolean update) {
        return isElectionInProgress.compareAndSet(expect, update);
    }

    public long getLastLeaderContactTime() {
        return lastLeaderContactTime.get();
    }

    public void resetLeaderContactTime() {
        lastLeaderContactTime.set(System.currentTimeMillis());
    }
}
