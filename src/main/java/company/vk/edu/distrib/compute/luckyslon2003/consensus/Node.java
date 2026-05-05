package company.vk.edu.distrib.compute.luckyslon2003.consensus;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class Node implements Runnable {

    static final long ELECTION_TIMEOUT_MS = 1_500;
    static final long PING_INTERVAL_MS = 1_000;
    static final long PING_TIMEOUT_MS = 2_000;
    static final long CHAOS_INTERVAL_MS = 5_000;
    static final double FAILURE_PROBABILITY = 0.15;
    static final long RECOVERY_DELAY_MS = 6_000;

    private final int id;
    private Map<Integer, Node> peers;
    private final BlockingQueue<Message> inbox = new LinkedBlockingQueue<>();
    private final AtomicReference<NodeState> state = new AtomicReference<>(NodeState.FOLLOWER);
    private final AtomicInteger leaderId = new AtomicInteger(-1);
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final ReentrantLock electionLock = new ReentrantLock();
    private boolean waitingForElectionAnswer;
    private long electionStartedAt;
    private final ReentrantLock pingLock = new ReentrantLock();
    private long lastPingResponseAt = System.currentTimeMillis();
    private final ScheduledExecutorService scheduler;

    public Node(int id) {
        this.id = id;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "node-" + id + "-scheduler");
            t.setDaemon(true);
            return t;
        });
    }

    public void setPeers(Map<Integer, Node> peers) {
        this.peers = peers;
    }

    public int getId() {
        return id;
    }

    public NodeState getState() {
        return state.get();
    }

    public int getLeaderId() {
        return leaderId.get();
    }

    public void forceDown() {
        if (state.getAndSet(NodeState.DOWN) != NodeState.DOWN) {
            log("Forced DOWN");
            inbox.clear();
        }
    }

    public void forceRecover() {
        if (state.compareAndSet(NodeState.DOWN, NodeState.FOLLOWER)) {
            log("Forced RECOVER – starting election");
            startElection();
        }
    }

    public void deliver(Message msg) {
        if (state.get() == NodeState.DOWN) {
            return;
        }
        inbox.offer(msg);
    }

    @Override
    public void run() {
        scheduler.scheduleAtFixedRate(this::pingTask, PING_INTERVAL_MS, PING_INTERVAL_MS,
                TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(this::chaosTask, CHAOS_INTERVAL_MS, CHAOS_INTERVAL_MS,
                TimeUnit.MILLISECONDS);

        while (!stopped.get()) {
            try {
                Message msg = inbox.poll(200, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    processMessage(msg);
                }
                checkElectionTimeout();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        scheduler.shutdownNow();
        log("Stopped");
    }

    private void processMessage(Message msg) {
        if (state.get() == NodeState.DOWN) {
            return;
        }

        switch (msg.getType()) {
            case PING -> handlePing(msg);
            case ANSWER -> handleAnswer(msg);
            case ELECT -> handleElect(msg);
            case VICTORY -> handleVictory(msg);
        }
    }

    private void handlePing(Message msg) {
        if (state.get() == NodeState.LEADER) {
            send(msg.getSenderId(), Message.Type.ANSWER);
        }
    }

    private void handleAnswer(Message msg) {
        pingLock.lock();
        try {
            lastPingResponseAt = System.currentTimeMillis();
        } finally {
            pingLock.unlock();
        }

        electionLock.lock();
        try {
            if (waitingForElectionAnswer) {
                log("Received ANSWER from node " + msg.getSenderId() + " – backing off election");
                waitingForElectionAnswer = false;
                state.compareAndSet(NodeState.ELECTION, NodeState.FOLLOWER);
            }
        } finally {
            electionLock.unlock();
        }
    }

    private void handleElect(Message msg) {
        send(msg.getSenderId(), Message.Type.ANSWER);
        log("Received ELECT from node " + msg.getSenderId()
                + " – responding ANSWER and starting own election");
        startElection();
    }

    private void handleVictory(Message msg) {
        int newLeader = msg.getSenderId();
        leaderId.set(newLeader);
        electionLock.lock();
        try {
            waitingForElectionAnswer = false;
        } finally {
            electionLock.unlock();
        }
        pingLock.lock();
        try {
            lastPingResponseAt = System.currentTimeMillis();
        } finally {
            pingLock.unlock();
        }
        if (newLeader == id) {
            state.set(NodeState.LEADER);
            log("I am the new LEADER");
        } else {
            state.compareAndSet(NodeState.ELECTION, NodeState.FOLLOWER);
            state.compareAndSet(NodeState.LEADER, NodeState.FOLLOWER);
            log("Acknowledged new LEADER: node " + newLeader);
        }
    }

    public void startElection() {
        if (state.get() == NodeState.DOWN) {
            return;
        }

        electionLock.lock();
        try {
            if (waitingForElectionAnswer) {
                electionStartedAt = System.currentTimeMillis();
                return;
            }

            state.set(NodeState.ELECTION);
            log("Starting election");

            List<Integer> higherNodes = peers.keySet().stream()
                    .filter(nid -> nid > id)
                    .toList();

            if (higherNodes.isEmpty()) {
                declareVictory();
                return;
            }

            for (int nid : higherNodes) {
                send(nid, Message.Type.ELECT);
            }

            waitingForElectionAnswer = true;
            electionStartedAt = System.currentTimeMillis();
        } finally {
            electionLock.unlock();
        }
    }

    private void checkElectionTimeout() {
        electionLock.lock();
        try {
            if (waitingForElectionAnswer
                    && System.currentTimeMillis() - electionStartedAt > ELECTION_TIMEOUT_MS) {
                log("Election timeout – no higher node responded, declaring VICTORY");
                declareVictory();
            }
        } finally {
            electionLock.unlock();
        }
    }

    private void declareVictory() {
        electionLock.lock();
        try {
            waitingForElectionAnswer = false;
        } finally {
            electionLock.unlock();
        }
        leaderId.set(id);
        state.set(NodeState.LEADER);
        log("Broadcasting VICTORY");
        for (int nid : peers.keySet()) {
            if (nid != id) {
                send(nid, Message.Type.VICTORY);
            }
        }
    }

    private void pingTask() {
        if (state.get() == NodeState.DOWN || state.get() == NodeState.LEADER) {
            return;
        }

        int currentLeader = leaderId.get();
        if (currentLeader < 0) {
            startElection();
            return;
        }

        send(currentLeader, Message.Type.PING);

        pingLock.lock();
        try {
            long silence = System.currentTimeMillis() - lastPingResponseAt;
            if (silence > PING_TIMEOUT_MS) {
                log("Leader node " + currentLeader + " is unresponsive (silence "
                        + silence + " ms) – starting election");
                leaderId.set(-1);
                startElection();
            }
        } finally {
            pingLock.unlock();
        }
    }

    private void chaosTask() {
        NodeState current = state.get();
        if (current == NodeState.DOWN) {
            return;
        }

        if (Math.random() < FAILURE_PROBABILITY) {
            log("CHAOS: random failure triggered");
            forceDown();
            scheduler.schedule(this::forceRecover, RECOVERY_DELAY_MS, TimeUnit.MILLISECONDS);
        }
    }

    private void send(int targetId, Message.Type type) {
        Node target = peers.get(targetId);
        if (target != null) {
            target.deliver(new Message(type, id));
        }
    }

    void log(String msg) {
        java.util.logging.Logger.getLogger(Node.class.getName())
                .info(String.format("[%6d ms] Node %2d [%-8s] leader=%-2s | %s%n",
                        System.currentTimeMillis() % 1_000_000,
                        id,
                        state.get(),
                        leaderId.get() < 0 ? "?" : leaderId.get(),
                        msg));
    }

    public void stop() {
        stopped.set(true);
        scheduler.shutdownNow();
    }
}
