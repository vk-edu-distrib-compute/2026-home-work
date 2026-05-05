package company.vk.edu.distrib.compute.maryarta.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import static company.vk.edu.distrib.compute.maryarta.consensus.MessageType.*;

public class Node implements Runnable{
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private static final int PING_TIMEOUT = 1000;
    private static final long ELECTION_TIMEOUT_MS = 1000;
    private static final double FAILURE_PROBABILITY = 0.20; // 10%
    private static final long FAILURE_CHECK_INTERVAL_MS = 3000;

    private static final long MIN_RECOVERY_DELAY_MS = 2000;
    private static final long MAX_RECOVERY_DELAY_MS = 6000;

    private final int id;
    private final BlockingQueue<Message> inbox = new LinkedBlockingQueue<>();
    private final Map<Integer, Node> nodes;
    private volatile boolean isEnable = true;

    private volatile int leaderID = -1;

    private long pingDeadlineAt = 0;
    private boolean waitingLeaderAnswer = false;

    private boolean electionInProgress = false;
    private int electionRound = 0;
    private final AtomicBoolean receivedAnswerOnElect = new AtomicBoolean(false);

    private static final Logger log = LoggerFactory.getLogger("node");

    public Node(Map<Integer, Node> nodes, int id) {
        this.nodes = nodes;
        this.id = id;
        log.info("Node {} started", id);
    }

    public void start(){
        isEnable = true;
        scheduler.scheduleWithFixedDelay(this::failRandomly, FAILURE_CHECK_INTERVAL_MS, FAILURE_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(this::checkLeaderAvailability, 0, 5, TimeUnit.SECONDS);
    }

    private synchronized void failRandomly() {
        if (!isEnable) {
            return;
        }
        double value = ThreadLocalRandom.current().nextDouble();
        if (value >= FAILURE_PROBABILITY) {
            return;
        }
        long recoveryDelayMs = ThreadLocalRandom.current().nextLong(
                MIN_RECOVERY_DELAY_MS,
                MAX_RECOVERY_DELAY_MS + 1
        );
        failFor(recoveryDelayMs);
    }

    public synchronized void failFor(long recoveryDelayMs) {
        if (!isEnable) {
            return;
        }
        stop();
        scheduler.schedule(
                this::recover,
                recoveryDelayMs,
                TimeUnit.MILLISECONDS
        );
    }

    private void sendMessage(MessageType messageType, int toId, MessageType answerTo){
        Node target = nodes.get(toId);
        if (target == null) {
            log.warn("Node {} cannot send {} to {}: target not found", id, messageType, toId);
            return;
        }
        Message message = new Message(messageType, id, toId, answerTo);
        target.receive(message);
    }

    private void handlePing(int fromId){ // лидер отвечает на пинг
        sendMessage(ANSWER, fromId, PING);
        log.info("Leader {} answered to PING to {}", id, fromId);
    }

    private void handleElect(int fromId){ // старшая нода отвечает на запросы о голосовании
        if (fromId >= this.id) {
            return;
        }
        sendMessage(ANSWER, fromId, ELECT);
        log.info("Node {} answered to ELECT to {}", id, fromId);
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
        if (fromId != leaderID) {
            return;
        }
        long now = System.currentTimeMillis();
        waitingLeaderAnswer = false;

        if (now >= pingDeadlineAt) {
            log.warn("Node {} received late PING answer from leader {}. Start election", id, leaderID);
            leaderID = -1;
            startElection();
            return;
        }
//        log.info("Node {} checked leader {} availability", id, leaderID);
    }
    private synchronized void handleElectAnswer(int fromId) {
        if (!electionInProgress) {
            return;
        }
        if (fromId <= this.id) {
            return;
        }
        log.info("Node {} got ELECT answer from {}", id, fromId);
        receivedAnswerOnElect.set(true);
    }

    private synchronized void handleVictory(int leaderID){
        this.leaderID = leaderID;
        receivedAnswerOnElect.set(false);
        waitingLeaderAnswer = false;
        electionInProgress = false;
        log.info("Node {} accepted leader {}", id, leaderID);
    }

    public void receive(Message message) {
        if (!isEnable) {
            return;
        }
        inbox.offer(message); // сообщение попадает во входящую очередь
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

    private void handleMessage(Message message){
        switch (message.type){
            case PING -> handlePing(message.fromId);
            case ELECT -> handleElect(message.fromId);
            case ANSWER -> handleAnswer(message.answerTo, message.fromId);
            case VICTORY -> handleVictory(message.fromId);
        }
    }

    private synchronized void checkLeaderAvailability() {
        if (!isEnable) {
            return;
        }
        if (id == leaderID) {
            return;
        }
        if (leaderID <= 0) {
            startElection();
            return;
        }

        long now = System.currentTimeMillis();
        if (!waitingLeaderAnswer) {
            sendMessage(MessageType.PING, leaderID, null);

            waitingLeaderAnswer = true;
            pingDeadlineAt = now + PING_TIMEOUT;

            log.info("Node {} sent PING to leader {}", id, leaderID);
            return;
        }

        if (now >= pingDeadlineAt) {
            log.warn("Node {} detected leader {} failure", id, leaderID);
            waitingLeaderAnswer = false;
            leaderID = -1;
            startElection();
        }
    }

    private synchronized void startElection(){
        if (!isEnable) {
            return;
        }
        if (electionInProgress) {
            return;
        }
        electionRound++;
        int currentRound = electionRound;
        electionInProgress = true;
        receivedAnswerOnElect.set(false);

        int electSent = 0;
        Set<Integer> ids = nodes.keySet();
        log.info("Node {} starts election. Known nodes: {}", this.id, nodes.keySet());
        for(int id: ids){
            if(id > this.id){
                sendMessage(ELECT, id, null);
                electSent++;
                log.info("Node {} send ELECT to {} ", this.id, id);
            }
        }
        if(electSent == 0){
            becomeLeader();
            return;
        }
        waitElectionAnswer(currentRound);

    }

    private void becomeLeader(){
        log.info("Node {} wants to become a leader", this.id);
        leaderID = id;
        electionInProgress = false;
        waitingLeaderAnswer = false;
        receivedAnswerOnElect.set(false);
        for(int target: nodes.keySet()){
            if (target != this.id){
                sendMessage(VICTORY, target, null);
                log.info("Node {} send victory to {} ", this.id, target);
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
        if (!receivedAnswerOnElect.get()) {
            log.info("Node {} did not receive ELECT answer in time", id);
            becomeLeader();
        } else {
            log.info("Node {} received ELECT answer in time, stays follower", id);
            electionInProgress = false;
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
        isEnable = enabled;
        leaderID = -1;
        waitingLeaderAnswer = false;
        electionInProgress = false;
        receivedAnswerOnElect.set(false);
        inbox.clear();
    }

    public int getLeaderID(){
        return leaderID;
    }

}
