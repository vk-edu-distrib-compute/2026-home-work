package company.vk.edu.distrib.compute.linempy.task5;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Класс Node реализует логику узла в распределенной системе с использованием алгоритма Bully.
 * Включает механизмы детекции отказов, проведения выборов и логирования времени работы алгоритма.
 */
public class Node implements Runnable {
    private static final String RESET = "\u001B[0m";
    private static final String RED = "\u001B[31m";
    private static final String GREEN = "\u001B[32m";
    private static final String YELLOW = "\u001B[33m";
    private static final String CYAN = "\u001B[36m";
    private static final String PURPLE = "\u001B[35m";

    private final int id;
    private final Map<Integer, Node> cluster;
    private final BlockingQueue<Message> messageQueue = new LinkedBlockingQueue<>();
    private final AtomicBoolean active = new AtomicBoolean(true);
    private final AtomicBoolean electionInProgress = new AtomicBoolean(false);

    private final AtomicLong lastAnswerReceived = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong lastElectionTime = new AtomicLong(0);
    private final AtomicLong electionStartTime = new AtomicLong(0);

    private static final long PING_INTERVAL = 3000;
    private static final long PING_TIMEOUT = 5000;
    private static final long ELECTION_TIMEOUT = 2000;
    private static final long MIN_ELECTION_INTERVAL = 3000;

    private volatile int currentLeaderId = -1;
    private volatile ScheduledFuture<?> electionTimeoutTask;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    public Node(int id, Map<Integer, Node> cluster) {
        this.id = id;
        this.cluster = cluster;
    }

    public int getId() {
        return id;
    }

    public boolean isActive() {
        return active.get();
    }

    public int getCurrentLeaderId() {
        return currentLeaderId;
    }

    private void log(String level, String msg, String color) {
        System.out.printf("%s[Node %d] [%-8s] %s%s\n", color, id, level, msg, RESET);
    }

    public void setStatus(boolean status) {
        if (status && !active.get()) {
            active.set(true);
            log("RESTORE", "Node recovered and rejoined the cluster.", GREEN);
            startElection();
        } else if (!status && active.get()) {
            active.set(false);
            log("DOWN", "Node crashed (failure).", RED);
            cancelElectionTimeout();
        }
    }

    public void receiveMessage(Message msg) {
        if (active.get()) {
            messageQueue.offer(msg);
        }
    }

    @Override
    public void run() {
        scheduler.scheduleAtFixedRate(this::pingLeader, 2000, PING_INTERVAL, TimeUnit.MILLISECONDS);

        try {
            while (!Thread.currentThread().isInterrupted()) {
                Message msg = messageQueue.poll(500, TimeUnit.MILLISECONDS);
                if (msg != null && active.get()) {
                    handleMessage(msg);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            scheduler.shutdownNow();
        }
    }

    private void handleMessage(Message msg) {
        switch (msg.type()) {
            case ELECT -> handleElect(msg);
            case ANSWER -> handleAnswer(msg);
            case VICTORY -> handleVictory(msg);
            case PING -> handlePing(msg);
        }
    }

    private void handleElect(Message msg) {
        if (msg.senderId() < this.id) {
            log("ELECT", "Received ELECT from " + msg.senderId() + ". Sending ANSWER.", YELLOW);
            sendTo(msg.senderId(), new Message(MessageType.ANSWER, this.id));

            if (!electionInProgress.get()) {
                startElection();
            }
        }
    }

    private void handleAnswer(Message msg) {
        log("ANSWER", "Received ANSWER from node " + msg.senderId() + ".", CYAN);
        cancelElectionTimeout();
        electionInProgress.set(false);
        lastAnswerReceived.set(System.currentTimeMillis());
    }

    private void handleVictory(Message msg) {
        this.currentLeaderId = msg.senderId();
        this.electionInProgress.set(false);
        cancelElectionTimeout();
        lastAnswerReceived.set(System.currentTimeMillis());

        long duration = (electionStartTime.get() > 0) ? (System.currentTimeMillis() - electionStartTime.get()) : 0;
        electionStartTime.set(0);

        String logMsg = String.format("Node %d is now the leader. (Election duration: %d ms)",
                msg.senderId(), duration);
        log("NEW LDR", logMsg, PURPLE);
    }

    private void handlePing(Message msg) {
        if (this.id == currentLeaderId) {
            sendTo(msg.senderId(), new Message(MessageType.ANSWER, this.id));
        }
    }

    private synchronized void startElection() {
        if (!active.get()) return;

        long now = System.currentTimeMillis();
        if (now - lastElectionTime.get() < MIN_ELECTION_INTERVAL) {
            log("ELECT", "Too frequent elections, skipping.", YELLOW);
            return;
        }

        if (electionInProgress.get()) return;

        log("ELECT", "Starting leader election (Bully algorithm)...", YELLOW);
        electionStartTime.set(now);
        lastElectionTime.set(now);
        electionInProgress.set(true);
        cancelElectionTimeout();

        boolean higherExists = false;
        for (Integer otherId : cluster.keySet()) {
            if (otherId > this.id) {
                Node otherNode = cluster.get(otherId);
                if (otherNode != null && otherNode.isActive()) {
                    higherExists = true;
                    sendTo(otherId, new Message(MessageType.ELECT, this.id));
                    log("ELECT", "Sent ELECT to node " + otherId, CYAN);
                }
            }
        }

        if (!higherExists) {
            becomeLeader();
        } else {
            electionTimeoutTask = scheduler.schedule(() -> {
                if (active.get() && electionInProgress.get()) {
                    log("TIMEOUT", "Higher nodes did not respond. Becoming leader.", YELLOW);
                    becomeLeader();
                }
            }, ELECTION_TIMEOUT, TimeUnit.MILLISECONDS);
        }
    }

    private void becomeLeader() {
        if (!active.get()) return;

        this.currentLeaderId = this.id;
        this.electionInProgress.set(false);
        cancelElectionTimeout();
        lastAnswerReceived.set(System.currentTimeMillis());

        long duration = System.currentTimeMillis() - electionStartTime.get();
        electionStartTime.set(0);

        String logMsg = String.format("I AM THE NEW LEADER! (Election duration: %d ms)", duration);
        log("LEADER", logMsg, GREEN);

        for (Node node : cluster.values()) {
            if (node.getId() != this.id && node.isActive()) {
                node.receiveMessage(new Message(MessageType.VICTORY, this.id));
            }
        }
    }

    private void pingLeader() {
        if (!active.get() || id == currentLeaderId) return;

        if (currentLeaderId == -1) {
            startElection();
            return;
        }

        Node leaderNode = cluster.get(currentLeaderId);
        if (leaderNode == null || !leaderNode.isActive()) {
            log("WARN", "Leader " + currentLeaderId + " is not active!", RED);
            startElection();
            return;
        }

        long now = System.currentTimeMillis();
        if (now - lastAnswerReceived.get() > PING_TIMEOUT) {
            log("WARN", "Leader " + currentLeaderId + " timed out (no PING response)!", RED);
            startElection();
        } else {
            sendTo(currentLeaderId, new Message(MessageType.PING, this.id));
            log("PING", "Sent PING to leader " + currentLeaderId, CYAN);
        }
    }

    private void sendTo(int targetId, Message msg) {
        Node target = cluster.get(targetId);
        if (target != null) {
            target.receiveMessage(msg);
        }
    }

    private void cancelElectionTimeout() {
        if (electionTimeoutTask != null && !electionTimeoutTask.isDone()) {
            electionTimeoutTask.cancel(false);
        }
    }

    public void stop() {
        log("SHUTDOWN", "Graceful shutdown initiated...", RED);
        if (id == currentLeaderId) {
            for (Node node : cluster.values()) {
                if (node.getId() != this.id && node.isActive()) {
                    node.receiveMessage(new Message(MessageType.ELECT, this.id));
                }
            }
        }
        active.set(false);
        scheduler.shutdownNow();
    }
}
