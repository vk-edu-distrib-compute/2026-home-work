package company.vk.edu.distrib.compute.arseniy90.consensus;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElectionManager.class.getName());

    private final int id;
    private final Map<Integer, Node> cluster;
    private final NodeState state;
    private final ElectionManager electionManager;

    public MessageProcessor(int id, Map<Integer, Node> cluster, NodeState state, ElectionManager electionManager) {
        this.id = id;
        this.cluster = cluster;
        this.state = state;
        this.electionManager = electionManager;
    }

    public void process(Message msg) {
        switch (msg.getType()) {
            case PING:
                handlePing(msg);
                break;
            case ELECT:
                handleElect(msg);
                break;
            case ANSWER:
                handleAnswer();
                break;
            case VICTORY:
                handleVictory(msg);
                break;
            case RESIGN:
                handleResign(msg);
                break;
        }
    }

    private void handlePing(Message msg) {
        if (state.getLeaderId() == id) {
            Node sender = cluster.get(msg.getSenderId());
            if (sender != null) {
                sender.receiveMessage(new Message(MessageType.ANSWER, id, "message processor: ping msg"));
            }
        }
    }

    private void handleElect(Message msg) {
        LOGGER.info("Node {}: got ELECT from node {}", id, msg.getSenderId());
        if (msg.getSenderId() < id) {
            Node senderNode = cluster.get(msg.getSenderId());
            if (senderNode != null) {
                senderNode.receiveMessage(new Message(MessageType.ANSWER, id, "message processor: elect msg"));
            }
            electionManager.startElection();
        }
    }

    private void handleAnswer() {
        state.resetLeaderContactTime();
        state.compareAndSetElection(true, false);
    }

    private void handleVictory(Message msg) {
        int senderId = msg.getSenderId();
        if (senderId < id && state.getRole() == NodeRole.CANDIDATE) {
            LOGGER.info("Node {}: got VICTORY from node with less id {}. Start elections.", id, senderId);
            state.setLeaderId(-1);
            state.setRole(NodeRole.FOLLOWER);
            electionManager.startElection();
            return;
        }

        if (senderId != id) {
            state.setLeaderId(senderId);
            state.resetLeaderContactTime();
            state.compareAndSetElection(true, false);
            state.setRole(NodeRole.FOLLOWER);
            LOGGER.info("Node {}: accepted new leader {}", id, state.getLeaderId());
        }
    }

    private void handleResign(Message msg) {
        LOGGER.info("Node {}: leader {} leaves. Start pre-term elections", id, msg.getSenderId());
        if (state.getLeaderId() == msg.getSenderId()) {
            state.setLeaderId(-1);
            state.setRole(NodeRole.FOLLOWER);
            electionManager.startElection();
        }
    }
}
