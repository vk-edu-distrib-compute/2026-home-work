package company.vk.edu.distrib.compute.tadzhnahal.consensus;

import java.lang.System.Logger;

final class NodeMessageHandler {
    private static final Logger LOG = System.getLogger(NodeMessageHandler.class.getName());

    private final Node node;

    NodeMessageHandler(Node node) {
        this.node = node;
    }

    void handle(Message message) {
        LogHelper.info(LOG, () -> node.getName() + " got " + message);

        if (message.getToId() != node.getNodeId()) {
            return;
        }

        switch (message.getType()) {
            case PING:
                handlePing(message);
                break;
            case ELECT:
                handleElect(message);
                break;
            case ANSWER:
                handleAnswer(message);
                break;
            case VICTORY:
                handleVictory(message);
                break;
            case SHUTDOWN:
                break;
        }
    }

    private void handlePing(Message message) {
        node.sendMessage(message.getFromId(), MessageType.ANSWER);
    }

    private void handleElect(Message message) {
        node.sendMessage(message.getFromId(), MessageType.ANSWER);
        node.startElection();
    }

    private void handleAnswer(Message message) {
        node.markAnswerFromHigher(message.getFromId());

        LogHelper.info(
                LOG,
                () -> node.getName() + " got answer from node " + message.getFromId()
        );
    }

    private void handleVictory(Message message) {
        int newLeaderId = message.getLeaderId();
        int oldLeaderId = node.getLeaderId();

        if (newLeaderId == Node.NO_LEADER) {
            return;
        }

        if (newLeaderId < node.getNodeId()) {
            node.startElection();
            return;
        }

        if (oldLeaderId > newLeaderId && node.isNodeWorking(oldLeaderId)) {
            return;
        }

        node.acceptLeader(newLeaderId);

        LogHelper.info(
                LOG,
                () -> node.getName() + " accepts leader " + newLeaderId
        );
    }
}
