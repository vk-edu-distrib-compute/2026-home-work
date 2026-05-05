package company.vk.edu.distrib.compute.andeco.election;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings({"PMD.CyclomaticComplexity", "PMD.AvoidUsingVolatile"})
public final class ElectionNode extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElectionNode.class);
    private static final double NO_FAILURE_PROBABILITY = 0.0;

    private final int nodeId;
    private final List<Integer> nodeIds;
    private final BlockingQueue<Message> queue = new LinkedBlockingQueue<>();
    private final Random random;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final ElectionConfig config;

    private volatile Cluster cluster;

    private volatile NodeRole nodeRole = NodeRole.SLAVE;
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

    public ElectionNode(int nodeId,
                        List<Integer> nodeIds,
                        ElectionConfig config,
                        ElectionNodeParameters parameters) {
        super("election-node-" + nodeId);
        this.nodeId = nodeId;
        this.nodeIds = List.copyOf(nodeIds);
        this.config = Objects.requireNonNullElseGet(
                config,
                () -> ElectionConfig.defaults(parameters.pingTimeoutMs(),
                        parameters.electionAnswerTimeoutMs(), parameters.victoryTimeoutMs())
        );
        this.failProbabilityPerTick = parameters.failProbabilityPerTick();
        this.minRecoverMs = parameters.minRecoverMs();
        this.maxRecoverMs = Math.max(parameters.maxRecoverMs(), parameters.minRecoverMs());
        this.random = new Random(parameters.randomSeed() * nodeId);
    }

    public int id() {
        return nodeId;
    }

    public void attachCluster(Cluster cluster) {
        this.cluster = Objects.requireNonNull(cluster);
    }

    public NodeRole role() {
        return nodeRole;
    }

    public void enqueue(Message message) {
        queue.offer(message);
    }

    public void shutdown() {
        running.set(false);
        interrupt();
    }

    public void forceDown() {
        if (nodeRole == NodeRole.DOWN) {
            return;
        }
        goDown("forced");
    }

    public void forceUp() {
        if (nodeRole != NodeRole.DOWN) {
            return;
        }
        recoverAtNs = 0;
        goUp("forced");
    }

    public void gracefulShutdown() {
        if (nodeRole == NodeRole.DOWN) {
            return;
        }
        if (nodeRole == NodeRole.LEADER) {
            long nextElectionId = election + 1;
            log("штатное отключение: передача роли");
            Cluster c = cluster;
            if (c != null) {
                c.broadcast(new Message(MessageType.STEP_DOWN, nodeId, nextElectionId));
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

        if (handleDownState(now)) {
            return;
        }

        maybeFail(now);

        if (handleLeaderState(now)) {
            return;
        }

        if (isInStepDownCooldown(now)) {
            return;
        }

        if (handleNoLeader(now)) {
            return;
        }

        maybeSendPing(now);

        if (checkLeaderTimeout(now)) {
            return;
        }

        ensureElectionProgress(now);
    }

    private boolean handleDownState(long now) {
        if (nodeRole != NodeRole.DOWN) {
            return false;
        }

        if (recoverAtNs != 0 && now >= recoverAtNs) {
            goUp("auto");
        }
        return true;
    }

    private boolean handleLeaderState(long now) {
        if (nodeRole != NodeRole.LEADER) {
            return false;
        }

        lastLeaderSeenNs = now;
        return true;
    }

    private boolean isInStepDownCooldown(long now) {
        return steppedDownUntilNs != 0 && now < steppedDownUntilNs;
    }

    private boolean handleNoLeader(long now) {
        if (leader != -1) {
            return false;
        }

        ensureElectionProgress(now);
        return true;
    }

    private void maybeSendPing(long now) {
        if (now - lastPingSentNs >= config.getPingPeriodNs()) {
            lastPingSentNs = now;
            send(leader, new Message(MessageType.PING, nodeId, election));
        }
    }

    private boolean checkLeaderTimeout(long now) {
        if (now - lastLeaderSeenNs <= config.getPingTimeoutNs()) {
            return false;
        }

        log("лидер " + leader + " недоступен по таймауту, старт выборов");
        leader = -1;
        startElection("leader-timeout");
        return true;
    }

    private void ensureElectionProgress(long now) {
        if (answerDeadlineNs != 0 && now >= answerDeadlineNs) {
            answerDeadlineNs = 0;
            if (hasAnswer) {
                victoryDeadlineNs = now + config.getVictoryTimeoutNs();
            } else {
                becomeLeaderAndBroadcast();
            }
        }

        if (victoryDeadlineNs != 0 && now >= victoryDeadlineNs) {
            victoryDeadlineNs = 0;
            hasAnswer = false;
            startElection("victory-timeout");
        }
    }

    private void onMessage(Message message) {
        if (nodeRole == NodeRole.DOWN) {
            return;
        }

        if (message.getElectionId() < election) {
            return;
        }

        switch (message.getType()) {
            case PING -> onPing(message);
            case ELECT -> onElect(message);
            case ANSWER -> onAnswer(message);
            case VICTORY -> onVictory(message);
            case STEP_DOWN -> onStepDown(message);
        }
    }

    private void onPing(Message message) {
        if (nodeRole == NodeRole.LEADER) {
            send(message.getFrom(), new Message(MessageType.ANSWER, nodeId, election));
        }
    }

    private void onElect(Message message) {
        if (message.getElectionId() > election) {
            election = message.getElectionId();
            hasAnswer = false;
            answerDeadlineNs = 0;
            victoryDeadlineNs = 0;
        }

        if (nodeId > message.getFrom()) {
            send(message.getFrom(), new Message(MessageType.ANSWER, nodeId, election));
            if (nodeRole != NodeRole.LEADER) {
                startElection("elect-from-" + message.getFrom());
            }
        }
    }

    private void onAnswer(Message message) {
        if (message.getElectionId() != election) {
            return;
        }

        if (answerDeadlineNs != 0) {
            hasAnswer = true;
        }

        if (leader != -1 && message.getFrom() == leader) {
            lastLeaderSeenNs = System.nanoTime();
        }
    }

    private void onVictory(Message message) {
        if (message.getElectionId() < election) {
            return;
        }

        election = message.getElectionId();
        answerDeadlineNs = 0;
        victoryDeadlineNs = 0;
        hasAnswer = false;

        if (message.getFrom() == nodeId) {
            nodeRole = NodeRole.LEADER;
            leader = nodeId;
            lastLeaderSeenNs = System.nanoTime();
            log("лидер подтверждён");
            return;
        }

        if (leader == -1 || message.getFrom() >= leader) {
            if (nodeRole == NodeRole.LEADER) {
                nodeRole = NodeRole.SLAVE;
            }
            leader = message.getFrom();
            lastLeaderSeenNs = System.nanoTime();
            log("лидер -> " + leader);
        }
    }

    private void onStepDown(Message message) {
        if (message.getElectionId() < election) {
            return;
        }
        if (message.getFrom() == leader || leader == -1) {
            leader = -1;
        }

        if (message.getElectionId() > election) {
            election = message.getElectionId();
        }

        steppedDownUntilNs = System.nanoTime() + config.getStepDownCooldownNs();
        startElection("step-down-from-" + message.getFrom());
    }

    private void startElection(String reason) {
        if (nodeRole == NodeRole.DOWN) {
            return;
        }

        steppedDownUntilNs = 0;
        long newElectionId = nextElectionId();
        if (newElectionId <= election) {
            newElectionId = election + 1;
        }
        election = newElectionId;

        nodeRole = NodeRole.SLAVE;
        leader = -1;
        hasAnswer = false;

        List<Integer> higherIds = higherNodeIds();
        if (higherIds.isEmpty()) {
            becomeLeaderAndBroadcast();
            return;
        }

        Message electMessage = new Message(MessageType.ELECT, nodeId, election);

        for (int target : higherIds) {
            send(target, electMessage);
        }

        answerDeadlineNs = System.nanoTime() + config.getAnswerTimeoutNs();
        victoryDeadlineNs = 0;
        log("выборы запущены (" + reason + "), наибольшие id: " + higherIds);
    }

    private void becomeLeaderAndBroadcast() {
        nodeRole = NodeRole.LEADER;
        leader = nodeId;
        lastLeaderSeenNs = System.nanoTime();
        answerDeadlineNs = 0;
        victoryDeadlineNs = 0;
        hasAnswer = false;

        log("победа: рассылка сообщения VICTORY");
        Cluster c = cluster;
        if (c != null) {
            c.broadcast(new Message(MessageType.VICTORY, nodeId, election));
        }
    }

    private List<Integer> higherNodeIds() {
        List<Integer> result = new ArrayList<>();
        for (int other : nodeIds) {
            if (other > nodeId) {
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
        if (failProbabilityPerTick <= NO_FAILURE_PROBABILITY) {
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
        nodeRole = NodeRole.DOWN;
        leader = -1;
        answerDeadlineNs = 0;
        victoryDeadlineNs = 0;
        hasAnswer = false;
        log("узел выключен (" + reason + ")");
    }

    private void goUp(String reason) {
        nodeRole = NodeRole.SLAVE;
        lastLeaderSeenNs = System.nanoTime();
        lastPingSentNs = 0;
        log("узел включен (" + reason + ")");
        startElection("recovery");
    }

    private long nextElectionId() {
        long ms = System.currentTimeMillis();
        return (ms << 16) | (nodeId & 0xFFFFL);
    }

    private void log(String message) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("узел={} роль={} лидер={} выборы={} :: {}",
                    nodeId, LocalisationUtils.roleRu(nodeRole), leader, election, message);
        }
    }

}

