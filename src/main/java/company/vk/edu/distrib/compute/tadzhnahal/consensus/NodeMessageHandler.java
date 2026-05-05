package company.vk.edu.distrib.compute.tadzhnahal.consensus;

import java.lang.System.Logger;

final class NodeMessageHandler {
    private static final Logger LOG = System.getLogger(NodeMessageHandler.class.getName());

    private final Node node;

    NodeMessageHandler(Node node) {
        this.node = node;
    }

    void handle(Message message) {
        if (!node.isWorking()) {
            return;
        }

        LOG.log(Logger.Level.INFO, node.getName() + " got " + message);

        if (message.getType() == MessageType.PING) {
            handlePing(message);
            return;
        }

        if (message.getType() == MessageType.ELECT) {
            handleElect(message);
            return;
        }

        if (message.getType() == MessageType.ANSWER) {
            handleAnswer(message);
            return;
        }

        if (message.getType() == MessageType.VICTORY) {
            handleVictory(message);
        }
    }

    private void handlePing(Message message) {
        node.sendMessage(message.getFromId(), MessageType.ANSWER);
    }

    private void handleElect(Message message) {
        if (message.getFromId() < node.getNodeId()) {
            node.sendMessage(message.getFromId(), MessageType.ANSWER);
            node.startElection();
        }
    }

    private void handleAnswer(Message message) {
        if (message.getFromId() == node.getLeaderId()) {
            node.rememberLeaderAnswer();
        }

        if (message.getFromId() > node.getNodeId()) {
            node.rememberAnswerFromHigherNode();
        }

        LOG.log(Logger.Level.INFO, node.getName() + " got answer from node " + message.getFromId());
    }

    private void handleVictory(Message message) {
        node.rememberLeader(message.getLeaderId());
        LOG.log(Logger.Level.INFO, node.getName() + " accepts leader " + message.getLeaderId());
    }
}
