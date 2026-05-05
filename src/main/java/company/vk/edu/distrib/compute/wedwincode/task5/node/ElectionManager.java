package company.vk.edu.distrib.compute.wedwincode.task5.node;

import company.vk.edu.distrib.compute.wedwincode.task5.ClusterLogger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

final class ElectionManager {
    private static final long ANSWER_TIMEOUT_MS = 700;
    private static final long ELECTION_TIMEOUT_MS = 1_500;
    private static final int INTERRUPTED_MARKER = Integer.MIN_VALUE;

    private final Node node;
    private final BlockingQueue<Message> messages;
    private final AtomicBoolean electionInProgress = new AtomicBoolean(false);

    private enum ElectionResult {
        WON, WAIT_FOR_VICTORY, LEADER_ALREADY_SELECTED, INTERRUPTED
    }

    ElectionManager(Node node, BlockingQueue<Message> messages) {
        this.node = node;
        this.messages = messages;
    }

    boolean isInProgress() {
        return electionInProgress.get();
    }

    void reset() {
        electionInProgress.set(false);
    }

    synchronized void startElection() {
        if (!canStartElection()) {
            return;
        }

        prepareElection();
        sendElectMessagesToHigherNodes();
        ElectionResult answerResult = waitForAnswersFromHigherNodes();

        if (answerResult == ElectionResult.INTERRUPTED || answerResult == ElectionResult.LEADER_ALREADY_SELECTED) {
            return;
        }

        if (answerResult == ElectionResult.WON) {
            becomeLeader();
            return;
        }

        ElectionResult victoryResult = waitForVictoryFromHigherNode();
        if (victoryResult == ElectionResult.INTERRUPTED || victoryResult == ElectionResult.LEADER_ALREADY_SELECTED) {
            return;
        }

        retryElectionIfLeaderWasNotSelected();
    }

    void becomeLeader() {
        node.setLeaderState(node.getId());
        electionInProgress.set(false);
        ClusterLogger.event(node.getId(), "became LEADER");
        Message victory = new Message(Message.Type.VICTORY, node.getId());

        for (Node otherNode : node.getCluster().values()) {
            if (otherNode.getId() != node.getId()) {
                node.sendMessageTo(otherNode, victory);
            }
        }
    }

    synchronized void forceBecomeLeaderAfterTransfer() {
        if (!node.isAlive()) {
            return;
        }

        node.setLeaderState(node.getId());
        electionInProgress.set(false);
        ClusterLogger.event(node.getId(), "became LEADER by graceful transfer");
    }

    private boolean canStartElection() {
        return node.isAlive() && !electionInProgress.get();
    }

    private void prepareElection() {
        electionInProgress.set(true);
        node.clearLeaderAndBecomeFollower();
        ClusterLogger.event(node.getId(), "started election");
    }

    private void sendElectMessagesToHigherNodes() {
        Message elect = new Message(Message.Type.ELECT, node.getId());
        for (Node otherNode : node.getCluster().values()) {
            sendElectMessageIfHigher(otherNode, elect);
        }
    }

    private void sendElectMessageIfHigher(Node otherNode, Message elect) {
        if (otherNode.getId() <= node.getId()) {
            return;
        }

        boolean delivered = node.sendMessageTo(otherNode, elect);
        if (!delivered) {
            ClusterLogger.event(node.getId(), "could not reach higher node " + otherNode.getId());
        }
    }

    private ElectionResult waitForAnswersFromHigherNodes() {
        boolean hasAnswerFromHigherNode = false;
        long deadline = System.currentTimeMillis() + ANSWER_TIMEOUT_MS;

        while (System.currentTimeMillis() < deadline) {
            Message message = pollMessageUntil(deadline);
            if (message == null) {
                break;
            }

            ElectionResult result = processElectionAnswerMessage(message);
            if (result == ElectionResult.WAIT_FOR_VICTORY) {
                hasAnswerFromHigherNode = true;
                continue;
            }

            if (result != null) {
                return result;
            }
        }

        return hasAnswerFromHigherNode ? ElectionResult.WAIT_FOR_VICTORY : ElectionResult.WON;
    }

    private ElectionResult waitForVictoryFromHigherNode() {
        long deadline = System.currentTimeMillis() + ELECTION_TIMEOUT_MS;

        while (System.currentTimeMillis() < deadline) {
            Message message = pollMessageUntil(deadline);

            if (message == null) {
                break;
            }

            ElectionResult result = processVictoryWaitMessage(message);
            if (result != null) {
                return result;
            }
        }

        return ElectionResult.WAIT_FOR_VICTORY;
    }

    private Message pollMessageUntil(long deadline) {
        long remaining = deadline - System.currentTimeMillis();

        if (remaining <= 0) {
            return null;
        }

        try {
            return messages.poll(remaining, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new Message(Message.Type.ANSWER, INTERRUPTED_MARKER);
        }
    }

    private ElectionResult processElectionAnswerMessage(Message message) {
        if (isInterruptedMarker(message)) {
            return ElectionResult.INTERRUPTED;
        }

        if (isAnswerFromHigherNode(message)) {
            ClusterLogger.event(node.getId(), "got ANSWER from higher node " + message.senderId());
            return ElectionResult.WAIT_FOR_VICTORY;
        }

        if (isVictoryFromHigherNode(message)) {
            node.handleVictory(message);
            return ElectionResult.LEADER_ALREADY_SELECTED;
        }

        node.handleMessage(message);

        return leaderWasSelected() ? ElectionResult.LEADER_ALREADY_SELECTED : null;
    }

    private ElectionResult processVictoryWaitMessage(Message message) {
        if (isInterruptedMarker(message)) {
            return ElectionResult.INTERRUPTED;
        }

        if (isVictoryFromHigherNode(message)) {
            node.handleVictory(message);
            return ElectionResult.LEADER_ALREADY_SELECTED;
        }

        node.handleMessage(message);

        return leaderWasSelected() ? ElectionResult.LEADER_ALREADY_SELECTED : null;
    }

    private boolean isInterruptedMarker(Message message) {
        return message.senderId() == INTERRUPTED_MARKER;
    }

    private boolean isAnswerFromHigherNode(Message message) {
        return message.type() == Message.Type.ANSWER && message.senderId() > node.getId();
    }

    private boolean isVictoryFromHigherNode(Message message) {
        return message.type() == Message.Type.VICTORY && message.senderId() > node.getId();
    }

    private boolean leaderWasSelected() {
        return node.getLeaderId() != -1 && !electionInProgress.get();
    }

    private void retryElectionIfLeaderWasNotSelected() {
        if (node.getLeaderId() != -1) {
            electionInProgress.set(false);
            return;
        }

        electionInProgress.set(false);
        node.receiveMessage(new Message(Message.Type.ELECT, node.getId()));
    }
}
