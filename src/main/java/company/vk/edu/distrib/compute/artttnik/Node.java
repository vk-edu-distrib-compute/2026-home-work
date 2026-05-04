package company.vk.edu.distrib.compute.artttnik;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Random;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Node extends Thread {
    private static final Logger LOGGER = Logger.getLogger(Node.class.getName());

    private final int id;
    private final Cluster cluster;
    private final BlockingQueue<Message> inbox = new LinkedBlockingQueue<>();
    private final Random rng = new Random();

    private final AtomicBoolean running = new AtomicBoolean(true);
    private boolean alive = true;

    private Integer leaderId;

    private final double failProbability;
    private final long tickMillis;
    private final long pingIntervalMillis;
    private final long electionTimeoutMillis;

    private final Lock electionLock = new ReentrantLock();
    private final Condition answerArrived = electionLock.newCondition();

    public Node(int id, Cluster cluster, double failProbability,
                long tickMillis, long pingIntervalMillis, long electionTimeoutMillis) {
        super("Node-" + id);
        this.id = id;
        this.cluster = cluster;
        this.failProbability = failProbability;
        this.tickMillis = tickMillis;
        this.pingIntervalMillis = pingIntervalMillis;
        this.electionTimeoutMillis = electionTimeoutMillis;
    }

    public int getNodeId() {
        return id;
    }

    public boolean isAliveNode() {
        return alive;
    }

    public Integer getLeaderId() {
        return leaderId;
    }

    public void receive(Message m) {
        if (!running.get()) {
            return;
        }
        if (!alive) {
            return;
        }
        inbox.offer(m);
    }

    public void forceDown() {
        alive = false;
        log("FORCED DOWN");
    }

    public void forceUp() {
        alive = true;
        log("FORCED UP");
    }

    public void shutdownNode() {
        running.set(false);
        interrupt();
    }

    private void log(String s) {
        String status = alive ? "UP" : "DOWN";
        LOGGER.info(System.currentTimeMillis() + " [" + getName() + "] " + status + " " + s);
    }

    @Override
    public void run() {
        long lastPing = System.currentTimeMillis();
        while (running.get()) {
            try {
                Message m = inbox.poll(tickMillis, TimeUnit.MILLISECONDS);
                if (m != null) {
                    handleMessage(m);
                }

                simulateFailure();

                lastPing = handleLeadershipLogic(lastPing);
            } catch (InterruptedException e) {
                interrupt();
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Node encountered exception", e);
            }
        }
    }

    private void simulateFailure() {
        if (rng.nextDouble() < failProbability) {
            alive = !alive;
            log(alive ? "Recovered (random)" : "Crashed (random)");
        }
    }

    private long handleLeadershipLogic(long lastPingParam) {
        if (!alive) {
            return lastPingParam;
        }
        long now = System.currentTimeMillis();
        long currentLastPing = lastPingParam;
        if (leaderId == null || leaderId == id) {
            if (leaderId == null) {
                startElection();
            }
        } else {
            if (now - currentLastPing >= pingIntervalMillis) {
                currentLastPing = now;
                sendPingToLeader();
            }
        }
        return currentLastPing;
    }

    private void handleMessage(Message m) {
        switch (m.type) {
            case PING: {
                if (alive && leaderId != null && leaderId == id) {
                    cluster.send(id, m.fromId, Message.answer(id));
                }
                break;
            }
            case ELECT: {
                if (alive && this.id > m.fromId) {
                    cluster.send(id, m.fromId, Message.answer(id));
                    log("Received ELECT from " + m.fromId + ", answered and starting election");
                    startElection();
                }
                break;
            }
            case ANSWER: {
                electionLock.lock();
                try {
                    answerArrived.signalAll();
                } finally {
                    electionLock.unlock();
                }
                break;
            }
            case VICTORY: {
                this.leaderId = m.leaderId;
                log("Accepted VICTORY from " + m.fromId + ", leader=" + m.leaderId);
                break;
            }
        }
    }

    private void sendPingToLeader() {
        Integer leader = leaderId;
        if (leader == null) {
            startElection();
            return;
        }
        cluster.send(id, leader, Message.ping(id));
        boolean answered = waitForAnswer(electionTimeoutMillis);
        if (!answered) {
            log("Leader " + leader + " did not answer ping, starting election");
            startElection();
        }
    }

    private boolean waitForAnswer(long timeoutMillis) {
        electionLock.lock();
        try {
            try {
                return answerArrived.await(timeoutMillis, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                interrupt();
                return false;
            }
        } finally {
            electionLock.unlock();
        }
    }

    private void startElection() {
        if (!alive) {
            return;
        }
        log("Starting election");
        boolean hasHigher = false;
        for (int peerId : cluster.nodeIds()) {
            if (peerId > this.id) {
                hasHigher = true;
                cluster.send(id, peerId, Message.elect(id));
            }
        }
        if (!hasHigher) {
            becomeLeader();
            return;
        }

        boolean answered = waitForAnswer(electionTimeoutMillis);
        if (answered) {
            log("Got ANSWER; waiting for VICTORY");
            return;
        }
        becomeLeader();
    }

    private void becomeLeader() {
        leaderId = id;
        log("BECOMES LEADER");
        for (int peerId : cluster.nodeIds()) {
            if (peerId != id) {
                cluster.send(id, peerId, Message.victory(id, id));
            }
        }
    }
}
