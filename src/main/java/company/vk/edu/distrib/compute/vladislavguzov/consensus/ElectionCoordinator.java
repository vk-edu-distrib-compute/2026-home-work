package company.vk.edu.distrib.compute.vladislavguzov.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

class ElectionCoordinator {

    private static final Logger log = LoggerFactory.getLogger(ElectionCoordinator.class);

    static final int PING_INTERVAL_MS = 500;
    static final int PING_TIMEOUT_MS = 1500;
    static final int ELECTION_TIMEOUT_MS = 1000;

    private final int nodeId;
    private final ElectionState state;
    private List<Node> peers;

    ElectionCoordinator(int nodeId) {
        this.nodeId = nodeId;
        this.state = new ElectionState();
    }

    void setPeers(List<Node> peers) {
        this.peers = List.copyOf(peers);
    }

    boolean isDown() {
        return state.isDown();
    }

    NodeState getCurrentState() {
        return state.get();
    }

    int getLeaderId() {
        return state.getLeaderId();
    }

    boolean markDown() {
        if (state.isDown()) {
            return false;
        }
        state.markDown();
        return true;
    }

    boolean recover() {
        if (!state.isDown()) {
            return false;
        }
        state.resetForRecovery();
        startElection();
        return true;
    }

    void handleMessage(Message msg) {
        switch (msg.type()) {
            case PING -> handlePing(msg);
            case ELECT -> handleElect(msg);
            case ANSWER -> handleAnswer(msg);
            case VICTORY -> handleVictory(msg);
        }
    }

    void onTick() {
        switch (state.get()) {
            case FOLLOWER -> tickFollower();
            case CANDIDATE -> tickCandidate();
            default -> { }
        }
    }

    private void handlePing(Message msg) {
        if (state.get() == NodeState.LEADER) {
            sendTo(msg.senderId(), new Message(MessageType.ANSWER, nodeId));
        }
    }

    private void handleElect(Message msg) {
        if (nodeId <= msg.senderId()) {
            return;
        }
        sendTo(msg.senderId(), new Message(MessageType.ANSWER, nodeId));
        NodeState current = state.get();
        if (current == NodeState.FOLLOWER) {
            startElection();
        } else if (current == NodeState.LEADER) {
            sendTo(msg.senderId(), new Message(MessageType.VICTORY, nodeId));
        }
    }

    private void handleAnswer(Message msg) {
        if (state.get() == NodeState.CANDIDATE) {
            state.waitingForAnswer = true;
        } else if (state.get() == NodeState.FOLLOWER && state.leaderId == msg.senderId()) {
            state.lastContactTime = System.currentTimeMillis();
        }
    }

    private void handleVictory(Message msg) {
        if (msg.senderId() > nodeId) {
            log.info("Node {}: accepting leader {}", nodeId, msg.senderId());
            state.becomeFollower(msg.senderId());
        } else if (msg.senderId() < nodeId && state.get() != NodeState.LEADER) {
            startElection();
        }
    }

    private void tickFollower() {
        if (state.leaderId < 0) {
            startElection();
            return;
        }
        long now = System.currentTimeMillis();
        if (now - state.lastPingTime >= PING_INTERVAL_MS) {
            sendTo(state.leaderId, new Message(MessageType.PING, nodeId));
            state.lastPingTime = now;
        }
        if (state.lastContactTime > 0 && now - state.lastContactTime > PING_TIMEOUT_MS) {
            log.info("Node {}: leader {} timed out, starting election", nodeId, state.leaderId);
            startElection();
        }
    }

    private void tickCandidate() {
        long now = System.currentTimeMillis();
        if (now - state.electionStartTime <= ELECTION_TIMEOUT_MS) {
            return;
        }
        if (state.waitingForAnswer) {
            state.waitingForAnswer = false;
            startElection();
        } else {
            declareVictory();
        }
    }

    void startElection() {
        log.info("Node {}: starting election", nodeId);
        state.becomeCandidate();
        boolean hasSeniors = false;
        for (Node peer : peers) {
            if (peer.getNodeId() > nodeId) {
                hasSeniors = true;
                peer.deliver(new Message(MessageType.ELECT, nodeId));
            }
        }
        if (!hasSeniors) {
            declareVictory();
        }
    }

    private void declareVictory() {
        log.info("Node {}: VICTORY — I am the new leader", nodeId);
        state.becomeLeader(nodeId);
        for (Node peer : peers) {
            peer.deliver(new Message(MessageType.VICTORY, nodeId));
        }
    }

    private void sendTo(int targetId, Message msg) {
        for (Node peer : peers) {
            if (peer.getNodeId() == targetId) {
                peer.deliver(msg);
                return;
            }
        }
    }
}
