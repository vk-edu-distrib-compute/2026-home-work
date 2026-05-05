package company.vk.edu.distrib.compute.vladislavguzov.consensus;

import java.util.concurrent.atomic.AtomicReference;

class ElectionState {

    final AtomicReference<NodeState> nodeState = new AtomicReference<>(NodeState.FOLLOWER);

    int leaderId = -1;
    long lastPingTime;
    long lastContactTime;
    long electionStartTime;
    boolean waitingForAnswer;

    public NodeState get() {
        return nodeState.get();
    }

    public boolean isDown() {
        return nodeState.get() == NodeState.DOWN;
    }

    public int getLeaderId() {
        return leaderId;
    }

    void becomeFollower(int newLeaderId) {
        leaderId = newLeaderId;
        lastContactTime = System.currentTimeMillis();
        waitingForAnswer = false;
        nodeState.set(NodeState.FOLLOWER);
    }

    void becomeCandidate() {
        leaderId = -1;
        electionStartTime = System.currentTimeMillis();
        waitingForAnswer = false;
        nodeState.set(NodeState.CANDIDATE);
    }

    void becomeLeader(int nodeId) {
        leaderId = nodeId;
        nodeState.set(NodeState.LEADER);
    }

    void markDown() {
        waitingForAnswer = false;
        nodeState.set(NodeState.DOWN);
    }

    void resetForRecovery() {
        leaderId = -1;
        lastPingTime = 0;
        lastContactTime = 0;
        electionStartTime = 0;
        waitingForAnswer = false;
        nodeState.set(NodeState.FOLLOWER);
    }
}
