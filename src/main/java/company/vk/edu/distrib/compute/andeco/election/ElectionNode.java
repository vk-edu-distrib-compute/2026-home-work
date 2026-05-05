package company.vk.edu.distrib.compute.andeco.election;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ElectionNode extends Thread {
    private final int id;
    private final List<Integer> nodeIds;
    private final BlockingQueue<Message> queue = new LinkedBlockingQueue<>();
    private final Random random;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final ElectionConfig config;

    private volatile Cluster cluster;

    private volatile NodeRole role = NodeRole.SLAVE;
    private volatile Integer leader;

    private volatile long election;
    private volatile long answerDeadlineNs;
    private volatile long victoryDeadlineNs;
    private volatile boolean hasAnswer;

    private volatile long lastPingSentNs;
    private volatile long lastLeaderSeenNs;
    private volatile long steppedDownUntilNs;

    private final double failProbabilityPerTick;
    private final long minRecoverMs;
    private final long maxRecoverMs;

    private volatile long recoverAtNs;

    public ElectionNode(int id,
                        List<Integer> nodeIds,
                        ElectionConfig config,
                        long pingTimeoutMs,
                        long electionAnswerTimeoutMs,
                        long victoryTimeoutMs,
                        double failProbabilityPerTick,
                        long minRecoverMs,
                        long maxRecoverMs,
                        long randomSeed) {
        super("election-node-" + id);
        this.id = id;
        this.nodeIds = List.copyOf(nodeIds);
        this.config = Objects.requireNonNullElseGet(
                config,
                () -> ElectionConfig.defaults(pingTimeoutMs, electionAnswerTimeoutMs, victoryTimeoutMs)
        );
        this.failProbabilityPerTick = failProbabilityPerTick;
        this.minRecoverMs = minRecoverMs;
        this.maxRecoverMs = Math.max(maxRecoverMs, minRecoverMs);
        this.random = new Random(randomSeed ^ id);
    }

    public int id() {
        return id;
    }

    public void attachCluster(Cluster cluster) {
        this.cluster = Objects.requireNonNull(cluster);
    }

    public NodeRole role() {
        return role;
    }

    public Integer leader() {
        return leader;
    }

    public void enqueue(Message message) {
        queue.offer(message);
    }

    public void shutdown() {
        running.set(false);
        interrupt();
    }

    public void forceDown() {
        if (role == NodeRole.DOWN) {
            return;
        }
        goDown("forced");
    }

    public void forceUp() {
        if (role != NodeRole.DOWN) {
            return;
        }
        recoverAtNs = 0;
        goUp("forced");
    }

    public void gracefulShutdown() {
        if (role == NodeRole.DOWN) {
            return;
        }
        if (role == NodeRole.LEADER) {
            long nextElectionId = election + 1;
            log("штатное отключение: передача роли");
            Cluster c = cluster;
            if (c != null) {
                c.broadcast(new Message(MessageType.STEP_DOWN, id, nextElectionId));
            }
        }
        goDown("graceful");
    }

    @Override
    public void run() {
        lastLeaderSeenNs = System.nanoTime();
        startElection("startup");

        while (running.get()) {
            try {
                Message message = queue.poll(config.loopPollMs(), TimeUnit.MILLISECONDS);
                if (message != null) {
                    onMessage(message);
                }
                onTick();
            } catch (InterruptedException ignored) {
                onTick();
            } catch (RuntimeException e) {
                log("ошибка: " + e.getClass().getSimpleName() + " " + e.getMessage());
                goDown("exception");
            }
        }
    }

    private void onTick() {
        long now = System.nanoTime();

        if (role == NodeRole.DOWN) {
            if (recoverAtNs != 0 && now >= recoverAtNs) {
                goUp("auto");
            }
            return;
        }

        maybeFail(now);

        if (role == NodeRole.LEADER) {
            lastLeaderSeenNs = now;
            return;
        }

        if (steppedDownUntilNs != 0 && now < steppedDownUntilNs) {
            return;
        }

        if (leader == null) {
            ensureElectionProgress(now);
            return;
        }

        if (now - lastPingSentNs >= config.pingPeriodNs()) {
            lastPingSentNs = now;
            send(leader, new Message(MessageType.PING, id, election));
        }

        if (now - lastLeaderSeenNs > config.pingTimeoutNs()) {
            log("лидер " + leader + " недоступен по таймауту, старт выборов");
            leader = null;
            startElection("leader-timeout");
            return;
        }

        ensureElectionProgress(now);
    }

    private void ensureElectionProgress(long now) {
        if (answerDeadlineNs != 0 && now >= answerDeadlineNs) {
            answerDeadlineNs = 0;
            if (!hasAnswer) {
                becomeLeaderAndBroadcast();
            } else {
                victoryDeadlineNs = now + config.victoryTimeoutNs();
            }
        }

        if (victoryDeadlineNs != 0 && now >= victoryDeadlineNs) {
            victoryDeadlineNs = 0;
            hasAnswer = false;
            startElection("victory-timeout");
        }
    }

    private void onMessage(Message message) {
        if (role == NodeRole.DOWN) {
            return;
        }

        if (message.election() < election) {
            return;
        }

        switch (message.type()) {
            case PING -> onPing(message);
            case ELECT -> onElect(message);
            case ANSWER -> onAnswer(message);
            case VICTORY -> onVictory(message);
            case STEP_DOWN -> onStepDown(message);
            default -> {
            }
        }
    }

    private void onPing(Message message) {
        if (role == NodeRole.LEADER) {
            send(message.from(), new Message(MessageType.ANSWER, id, election));
        }
    }

    private void onElect(Message message) {
        if (message.election() > election) {
            election = message.election();
            hasAnswer = false;
            answerDeadlineNs = 0;
            victoryDeadlineNs = 0;
        }

        if (id > message.from()) {
            send(message.from(), new Message(MessageType.ANSWER, id, election));
            if (role != NodeRole.LEADER) {
                startElection("elect-from-" + message.from());
            }
        }
    }

    private void onAnswer(Message message) {
        if (message.election() != election) {
            return;
        }

        if (answerDeadlineNs != 0) {
            hasAnswer = true;
        }

        if (leader != null && message.from() == leader) {
            lastLeaderSeenNs = System.nanoTime();
        }
    }

    private void onVictory(Message message) {
        if (message.election() < election) {
            return;
        }

        election = message.election();
        answerDeadlineNs = 0;
        victoryDeadlineNs = 0;
        hasAnswer = false;

        if (message.from() == id) {
            role = NodeRole.LEADER;
            leader = id;
            lastLeaderSeenNs = System.nanoTime();
            log("лидер подтверждён");
            return;
        }

        if (leader == null || message.from() >= leader) {
            if (role == NodeRole.LEADER) {
                role = NodeRole.SLAVE;
            }
            leader = message.from();
            lastLeaderSeenNs = System.nanoTime();
            log("лидер -> " + leader);
        }
    }

    private void onStepDown(Message message) {
        if (message.election() < election) {
            return;
        }
        if (message.from() == leader || leader == null) {
            leader = null;
        }

        if (message.election() > election) {
            election = message.election();
        }

        steppedDownUntilNs = System.nanoTime() + config.stepDownCooldownNs();
        startElection("step-down-from-" + message.from());
    }

    private void startElection(String reason) {
        if (role == NodeRole.DOWN) {
            return;
        }

        steppedDownUntilNs = 0;
        long newElectionId = nextElectionId();
        if (newElectionId <= election) {
            newElectionId = election + 1;
        }
        election = newElectionId;

        role = NodeRole.SLAVE;
        leader = null;
        hasAnswer = false;

        List<Integer> higherIds = higherNodeIds();
        if (higherIds.isEmpty()) {
            becomeLeaderAndBroadcast();
            return;
        }

        for (int target : higherIds) {
            send(target, new Message(MessageType.ELECT, id, election));
        }

        answerDeadlineNs = System.nanoTime() + config.answerTimeoutNs();
        victoryDeadlineNs = 0;
        log("выборы запущены (" + reason + "), наибольшие id: " + higherIds);
    }

    private void becomeLeaderAndBroadcast() {
        role = NodeRole.LEADER;
        leader = id;
        lastLeaderSeenNs = System.nanoTime();
        answerDeadlineNs = 0;
        victoryDeadlineNs = 0;
        hasAnswer = false;

        log("победа: рассылка сообщения VICTORY");
        Cluster c = cluster;
        if (c != null) {
            c.broadcast(new Message(MessageType.VICTORY, id, election));
        }
    }

    private List<Integer> higherNodeIds() {
        List<Integer> result = new ArrayList<>();
        for (int other : nodeIds) {
            if (other > id) {
                result.add(other);
            }
        }
        return result;
    }

    private void send(int toId, Message message) {
        Cluster c = cluster;
        if (c == null) {
            return;
        }
        c.send(toId, message);
    }

    private void maybeFail(long now) {
        if (failProbabilityPerTick <= 0.0) {
            return;
        }
        if (random.nextDouble() >= failProbabilityPerTick) {
            return;
        }
        goDown("auto");
        long recoverMs = minRecoverMs;
        if (maxRecoverMs > minRecoverMs) {
            recoverMs += random.nextInt((int) (maxRecoverMs - minRecoverMs + 1));
        }
        recoverAtNs = now + TimeUnit.MILLISECONDS.toNanos(recoverMs);
    }

    private void goDown(String reason) {
        role = NodeRole.DOWN;
        leader = null;
        answerDeadlineNs = 0;
        victoryDeadlineNs = 0;
        hasAnswer = false;
        log("узел выключен (" + reason + ")");
    }

    private void goUp(String reason) {
        role = NodeRole.SLAVE;
        lastLeaderSeenNs = System.nanoTime();
        lastPingSentNs = 0;
        log("узел включен (" + reason + ")");
        startElection("recovery");
    }

    private long nextElectionId() {
        long ms = System.currentTimeMillis();
        return (ms << 16) | (id & 0xFFFFL);
    }

    private void log(String message) {
        System.out.println("узел=" + id + " роль=" + LocalisationUtils.roleRu(role) + " лидер=" + leader + " выборы=" + election
                + " :: " + message);
    }

}

