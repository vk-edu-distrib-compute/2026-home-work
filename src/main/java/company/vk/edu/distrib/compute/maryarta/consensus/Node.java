package company.vk.edu.distrib.compute.maryarta.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static company.vk.edu.distrib.compute.maryarta.consensus.MessageType.*;

public class Node implements Runnable {
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private static final int PING_TIMEOUT_MS = 1000;
    private static final long ELECTION_TIMEOUT_MS = 1000;
    private static final double FAILURE_PROBABILITY = 0.20;
    private static final long FAILURE_CHECK_INTERVAL_MS = 3000;

    private static final long MIN_RECOVERY_DELAY_MS = 2000;
    private static final long MAX_RECOVERY_DELAY_MS = 6000;

    private final int id;
    private final BlockingQueue<Message> inbox = new LinkedBlockingQueue<>();
    private final Map<Integer, Node> nodes;
    private final AtomicBoolean isEnable = new AtomicBoolean();

    private final AtomicInteger leaderID = new AtomicInteger(-1);

    private long pingDeadlineAt;
    private boolean waitingLeaderAnswer;
    private boolean electionInProgress;
    private int electionRound;
    private final AtomicBoolean receivedAnswerOnElect = new AtomicBoolean(false);

    private static final Logger log = LoggerFactory.getLogger("node");

    public Node(Map<Integer, Node> nodes, int id) {
        this.nodes = nodes;
        this.id = id;
        log.info("Node {} started", id);
    }

    public void start() {
        isEnable.set(true);
        scheduler.scheduleWithFixedDelay(this::failRandomly,
                FAILURE_CHECK_INTERVAL_MS,
                FAILURE_CHECK_INTERVAL_MS,
                TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(this::checkLeaderAvailability, 0, 5, TimeUnit.SECONDS);
    }

    private synchronized void failRandomly() {
        if (!isEnable.get()) {
            return;
        }
        double value = ThreadLocalRandom.current().nextDouble();
        if (value >= FAILURE_PROBABILITY) {
            return;
        }
        long recoveryDelayMs = ThreadLocalRandom.current().nextLong(MIN_RECOVERY_DELAY_MS, MAX_RECOVERY_DELAY_MS + 1);
        failFor(recoveryDelayMs);
    }

    public synchronized void failFor(long recoveryDelayMs) {
        if (!isEnable.get()) {
            return;
        }
        stop();
        log.warn("Node {} failed for {} ms", id, recoveryDelayMs);
        scheduler.schedule(this::recover, recoveryDelayMs, TimeUnit.MILLISECONDS);
    }

    private void sendMessage(MessageType messageType, int toId, MessageType answerTo) {
        Node target = nodes.get(toId);
        if (target == null) {
            log.warn("Node {} cannot send {} to {}: target not found", id, messageType, toId);
            return;
        }
        Message message = new Message(messageType, id, toId, answerTo);
        target.receive(message);
    }

    private void handlePing(int fromId) {
        sendMessage(ANSWER, fromId, PING);
        log.info("Node {} answered to PING from {}", id, fromId);
    }

    private void handleElect(int fromId) {
        if (fromId >= this.id) {
            return;
        }
        sendMessage(ANSWER, fromId, ELECT);
        log.info("Node {} answered to ELECT from {}", id, fromId);
        if (leaderID.get() == this.id) {
            sendMessage(VICTORY, fromId, null);
            log.info("Node {} sent VICTORY to {}", id, fromId);
            return;
        }
        startElection();
    }

    private synchronized void handleAnswer(MessageType answerTo, int fromId) {
        if (answerTo == PING) {
            handlePingAnswer(fromId);
            return;
        }
        if (answerTo == ELECT) {
            handleElectAnswer(fromId);
        }
    }

    private synchronized void handlePingAnswer(int fromId) {
        if (!waitingLeaderAnswer) {
            return;
        }
        if (fromId != leaderID.get()) {
            return;
        }
        long now = System.currentTimeMillis();
        waitingLeaderAnswer = false;

        if (now >= pingDeadlineAt) {
            log.warn("Node {} received late PING answer from leader {}; starting election", id, leaderID);
            leaderID.set(-1);
            startElection();
        }
    }

    private synchronized void handleElectAnswer(int fromId) {
        if (!electionInProgress) {
            return;
        }
        if (fromId <= this.id) {
            return;
        }
        log.info("Node {} received ELECT answer from {}", id, fromId);
        receivedAnswerOnElect.set(true);
    }

    private synchronized void handleVictory(int leaderID) {
        this.leaderID.set(leaderID);
        receivedAnswerOnElect.set(false);
        waitingLeaderAnswer = false;
        electionInProgress = false;
        log.info("Node {} accepted leader {}", id, leaderID);
    }

    public void receive(Message message) {
        if (!isEnable.get()) {
            return;
        }
        inbox.offer(message);
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Message message = inbox.poll(100, TimeUnit.MILLISECONDS);
                if (message != null) {
                    handleMessage(message);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void handleMessage(Message message) {
        if (!isEnable.get()) {
            return;
        }
        switch (message.type) {
            case PING -> handlePing(message.fromId);
            case ELECT -> handleElect(message.fromId);
            case ANSWER -> handleAnswer(message.answerTo, message.fromId);
            case VICTORY -> handleVictory(message.fromId);
        }
    }

    private synchronized void checkLeaderAvailability() {
        if (!isEnable.get()) {
            return;
        }
        int currentLeaderId = leaderID.get();
        if (id == currentLeaderId) {
            return;
        }
        if (currentLeaderId <= 0) {
            startElection();
            return;
        }

        long now = System.currentTimeMillis();
        if (!waitingLeaderAnswer) {
            sendMessage(PING, currentLeaderId, null);
            waitingLeaderAnswer = true;
            pingDeadlineAt = now + PING_TIMEOUT_MS;

            log.info("Node {} sent PING to leader {}", id, currentLeaderId);
            return;
        }

        if (now >= pingDeadlineAt) {
            log.warn("Node {} detected leader {} failure", id, currentLeaderId);
            waitingLeaderAnswer = false;
            leaderID.set(-1);
            startElection();
        }
    }

    private synchronized void startElection() {
        if (!isEnable.get()) {
            return;
        }
        if (electionInProgress) {
            return;
        }
        electionRound++;

        electionInProgress = true;
        receivedAnswerOnElect.set(false);

        int electSent = 0;
        log.info("Node {} started election", this.id);
        for (int target: nodes.keySet()) {
            if (target > this.id) {
                sendMessage(ELECT, target, null);
                electSent++;
                log.info("Node {} sent ELECT to {}", this.id, target);
            }
        }
        if (electSent == 0) {
            becomeLeader();
            return;
        }
        int currentRound = electionRound;
        waitElectionAnswer(currentRound);
    }

    private synchronized void becomeLeader() {
        leaderID.set(id);
        electionInProgress = false;
        waitingLeaderAnswer = false;
        receivedAnswerOnElect.set(false);
        log.info("Node {} became leader", this.id);
        for (int target: nodes.keySet()) {
            if (target != this.id) {
                sendMessage(VICTORY, target, null);
                log.info("Node {} sent VICTORY to {}", this.id, target);
            }
        }

    }

    private void waitElectionAnswer(int round) {
        new Thread(() -> {
            try {
                Thread.sleep(ELECTION_TIMEOUT_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            checkElectionAnswerAfterTimeout(round);
        }, "election-timeout-" + id).start();
    }

    private synchronized void checkElectionAnswerAfterTimeout(int round) {
        if (!electionInProgress) {
            return;
        }
        if (round != electionRound) {
            return;
        }

        if (receivedAnswerOnElect.get()) {
            log.info("Node {} received ELECT answer in time; stays follower", id);
            electionInProgress = false;
        } else {
            log.info("Node {} did not receive ELECT answer in time", id);
            becomeLeader();
        }
    }

    public synchronized void stop() {
        setEnabled(false);
        log.warn("Node {} stopped", id);
    }

    public synchronized void recover() {
        setEnabled(true);
        log.info("Node {} recovered", id);
        startElection();
    }

    private void setEnabled(boolean enabled) {
        isEnable.set(enabled);
        leaderID.set(-1);
        waitingLeaderAnswer = false;
        electionInProgress = false;
        receivedAnswerOnElect.set(false);
        inbox.clear();
    }

    public int getLeaderID() {
        return leaderID.get();
    }

    public boolean isEnable() {
        return isEnable.get();
    }

    public int getId() {
        return id;
    }

}
