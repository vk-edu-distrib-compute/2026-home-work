package company.vk.edu.distrib.compute.aldor7705.hw5;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class Node implements Runnable {
    private static final long PING_INTERVAL_MS = 1_000;
    private static final long PING_TIMEOUT_MS = 2_500;
    private static final long ELECTION_TIMEOUT_MS = 2_000;
    private static final long MALFUNCTION_CHECK_INTERVAL_MS = 5_000;
    private static final long MIN_RECOVERY_DELAY_MS = 3_000;
    private static final long MAX_RECOVERY_DELAY_MS = 8_000;

    private final int id;
    private final AtomicReference<NodeState> state = new AtomicReference<>(NodeState.FOLLOWER);
    private final AtomicInteger leaderId = new AtomicInteger(-1);
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final DelayQueue<Message> inbox = new DelayQueue<>();
    private volatile Map<Integer, Node> peers;
    private Thread thread;
    private final ReentrantLock electionLock = new ReentrantLock();
    private final ReentrantLock pingLock = new ReentrantLock();
    private boolean electionInProgress = false;
    private long electionStartTime = 0;
    private long lastPingResponseTime = 0;
    private boolean pingInProgress = false;
    private final Random random = new Random();
    private volatile boolean malfunctionScheduled = false;
    private long lastMalfunctionCheck = 0;
    private static final double FAILURE_PROBABILITY = 0.15;

    public Node(int id) {
        this.id = id;
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

    public void deliver(Message msg) {
        if (state.get() == NodeState.DOWN) {
            return;
        }
        inbox.offer(msg);
    }

    public void forceDown() {
        NodeState prevState = state.getAndSet(NodeState.DOWN);
        if (prevState != NodeState.DOWN) {
            log(">>> ПРИНУДИТЕЛЬНО ОТКЛЮЧЁН <<<");
            inbox.clear();
            electionLock.lock();
            try {
                electionInProgress = false;
            } finally {
                electionLock.unlock();
            }
        }
    }

    public void forceRecover() {
        if (state.compareAndSet(NodeState.DOWN, NodeState.FOLLOWER)) {
            log(">>> ПРИНУДИТЕЛЬНО ВОССТАНОВЛЕН <<<");
            startElection();
        }
    }

    private void send(int targetId, MessageType type) {
        Node target = peers.get(targetId);
        if (target != null && target.getState() != NodeState.DOWN) {
            target.deliver(new Message(type, id));
        }
    }

    private void broadcast(MessageType type) {
        for (int peerId : peers.keySet()) {
            if (peerId != id) {
                send(peerId, type);
            }
        }
    }

    private void sendToHigherNodes(MessageType type) {
        for (int peerId : peers.keySet()) {
            if (peerId > id) {
                send(peerId, type);
            }
        }
    }

    private void scheduleSelfMessage(MessageType type, long delayMs) {
        inbox.offer(new Message(type, id, delayMs, type));
    }

    public void startElection() {
        if (state.get() == NodeState.DOWN) {
            return;
        }

        electionLock.lock();
        try {
            if (electionInProgress) {
                electionStartTime = System.currentTimeMillis();
                return;
            }

            if (state.get() == NodeState.LEADER) {
                return;
            }

            state.set(NodeState.CANDIDATE);
            electionInProgress = true;
            electionStartTime = System.currentTimeMillis();

            log("Запускаю выборы (алгоритм Bully)");

            List<Integer> higherNodes = peers.keySet().stream()
                    .filter(pid -> pid > id)
                    .filter(pid -> peers.get(pid).getState() != NodeState.DOWN)
                    .toList();

            if (higherNodes.isEmpty()) {
                declareVictory();
            } else {
                for (int pid : higherNodes) {
                    send(pid, MessageType.ELECT);
                }
                scheduleSelfMessage(MessageType.ELECT, ELECTION_TIMEOUT_MS);
            }
        } finally {
            electionLock.unlock();
        }
    }

    private void declareVictory() {
        electionLock.lock();
        try {
            electionInProgress = false;
            state.set(NodeState.LEADER);
            leaderId.set(id);
            log("*** ОБЪЯВЛЯЮ ПОБЕДУ - Я ТЕПЕРЬ ЛИДЕР ***");
            broadcast(MessageType.VICTORY);
            scheduleSelfMessage(MessageType.PING, PING_INTERVAL_MS);
        } finally {
            electionLock.unlock();
        }
    }

    private void processMessage(Message msg) {
        if (state.get() == NodeState.DOWN) {
            return;
        }

        switch (msg.getType()) {
            case PING -> handlePing(msg);
            case ELECT -> handleElect(msg);
            case ANSWER -> handleAnswer(msg);
            case VICTORY -> handleVictory(msg);
        }
    }

    private void handlePing(Message msg) {
        if (state.get() == NodeState.LEADER) {
            if (msg.getInResponseTo() == null) {
                send(msg.getSenderId(), MessageType.ANSWER);
            }
            if (msg.getSenderId() == id && msg.getInResponseTo() == MessageType.PING) {
                scheduleSelfMessage(MessageType.PING, PING_INTERVAL_MS);
            }
        } else if (msg.getInResponseTo() == MessageType.PING) {
            performPing();
        }
    }

    private void handleElect(Message msg) {
        send(msg.getSenderId(), MessageType.ANSWER);

        if (msg.getSenderId() == id && msg.getInResponseTo() == MessageType.ELECT) {
            electionLock.lock();
            try {
                if (electionInProgress &&
                        System.currentTimeMillis() - electionStartTime >= ELECTION_TIMEOUT_MS) {
                    log("Таймаут выборов - старшие узлы не ответили");
                    declareVictory();
                }
            } finally {
                electionLock.unlock();
            }
            return;
        }

        if (state.get() != NodeState.DOWN && state.get() != NodeState.CANDIDATE) {
            log("Получил ELECT от узла " + msg.getSenderId() + " - запускаю свои выборы");
            startElection();
        }
    }

    private void handleAnswer(Message msg) {
        pingLock.lock();
        try {
            lastPingResponseTime = System.currentTimeMillis();
            pingInProgress = false;
        } finally {
            pingLock.unlock();
        }

        electionLock.lock();
        try {
            if (electionInProgress && msg.getSenderId() > id) {
                log("Получил ANSWER от старшего узла " + msg.getSenderId() + " - отступаю");
                electionInProgress = false;
                state.set(NodeState.FOLLOWER);
            }
        } finally {
            electionLock.unlock();
        }
    }

    private void handleVictory(Message msg) {
        int newLeaderId = msg.getSenderId();

        electionLock.lock();
        try {
            electionInProgress = false;
        } finally {
            electionLock.unlock();
        }

        if (newLeaderId == id) {
            state.set(NodeState.LEADER);
        } else if (newLeaderId > id) {
            leaderId.set(newLeaderId);
            state.set(NodeState.FOLLOWER);

            pingLock.lock();
            try {
                lastPingResponseTime = System.currentTimeMillis();
                pingInProgress = false;
            } finally {
                pingLock.unlock();
            }

            log("Признаю нового лидера: узел " + newLeaderId);
        } else {
            log("Получил VICTORY от младшего узла " + newLeaderId + " - подозрительно, запускаю выборы");
            startElection();
        }
    }

    private void performPing() {
        if (state.get() == NodeState.DOWN || state.get() == NodeState.LEADER) {
            return;
        }

        int currentLeader = leaderId.get();
        if (currentLeader < 0) {
            log("Лидер неизвестен - запускаю выборы");
            startElection();
            return;
        }

        pingLock.lock();
        try {
            long silenceDuration = System.currentTimeMillis() - lastPingResponseTime;
            if (pingInProgress && silenceDuration > PING_TIMEOUT_MS) {
                log("Лидер " + currentLeader + " не отвечает уже " + silenceDuration + "мс");
                leaderId.set(-1);
                pingInProgress = false;
                startElection();
                return;
            }
        } finally {
            pingLock.unlock();
        }

        pingLock.lock();
        try {
            pingInProgress = true;
        } finally {
            pingLock.unlock();
        }

        send(currentLeader, MessageType.PING);
        scheduleSelfMessage(MessageType.PING, PING_INTERVAL_MS);
    }

    private void checkMalfunction() {
        long now = System.currentTimeMillis();
        if (now - lastMalfunctionCheck < MALFUNCTION_CHECK_INTERVAL_MS) {
            return;
        }
        lastMalfunctionCheck = now;

        NodeState current = state.get();
        if (current == NodeState.DOWN) {
            return;
        }

        if (random.nextDouble() < FAILURE_PROBABILITY && !malfunctionScheduled) {
            malfunctionScheduled = true;
            log("!!! СЛУЧАЙНЫЙ СБОЙ !!!");
            forceDown();

            long recoveryDelay = MIN_RECOVERY_DELAY_MS +
                    random.nextLong(MAX_RECOVERY_DELAY_MS - MIN_RECOVERY_DELAY_MS);

            new Thread(() -> {
                try {
                    Thread.sleep(recoveryDelay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                malfunctionScheduled = false;
                forceRecover();
            }, "recovery-" + id).start();
        }
    }

    @Override
    public void run() {
        log("Узел запущен");
        lastPingResponseTime = System.currentTimeMillis();

        while (!stopped.get()) {
            try {
                Message msg = inbox.poll(100, TimeUnit.MILLISECONDS);

                if (msg != null) {
                    processMessage(msg);
                }

                checkMalfunction();

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        log("Узел остановлен");
    }

    public void start() {
        thread = new Thread(this, "node-" + id);
        thread.start();
    }

    public void stop() {
        stopped.set(true);
        if (thread != null) {
            thread.interrupt();
        }
    }

    private void log(String msg) {
        System.out.printf("[%5dмс] Узел %2d [%-9s] лидер=%s | %s%n",
                System.currentTimeMillis() % 100_000,
                id,
                state.get(),
                leaderId.get() < 0 ? "?" : leaderId.get(),
                msg);
    }
}