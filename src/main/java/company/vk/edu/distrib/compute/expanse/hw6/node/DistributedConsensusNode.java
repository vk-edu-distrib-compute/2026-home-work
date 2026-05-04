package company.vk.edu.distrib.compute.expanse.hw6.node;



import company.vk.edu.distrib.compute.expanse.hw6.model.Message;
import company.vk.edu.distrib.compute.expanse.hw6.model.MessageType;
import company.vk.edu.distrib.compute.expanse.hw6.model.NodeState;
import company.vk.edu.distrib.compute.expanse.hw6.utils.ConcurrentUtils;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DistributedConsensusNode {
    private static final Logger logger = Logger.getLogger(DistributedConsensusNode.class.getName());

    private final TreeMap<Long, DistributedConsensusNode> nodesMap;
    private final BlockingQueue<Message> messages;
    private final Thread thread;
    private final AtomicBoolean isActive;
    private final ReentrantLock lock;
    private final long nodeId;
    private final long minDelayMillis;
    private final long maxDelayMillis;
    private final int malfunctionProbability;
    private final long malfunctionTimeoutMillis;
    private NodeState state;
    private Instant lastMalfunctionTime;
    private Long leaderId;
    private boolean receivedPing;
    private long term;

    public DistributedConsensusNode(
            long minDelayMillis,
            long maxDelayMillis,
            TreeMap<Long, DistributedConsensusNode> nodesMap,
            int malfunctionProbability,
            long malfunctionTimeoutMillis) {
        if (minDelayMillis <= 0 || maxDelayMillis <= 0 || minDelayMillis >= maxDelayMillis) {
            throw new IllegalArgumentException("Min and max delays must be positive and min < max");
        }
        if (malfunctionProbability < 0 || malfunctionProbability >= 100) {
            throw new IllegalArgumentException("Malfunction probability must be in range [0, 100)");
        }
        if (malfunctionTimeoutMillis < 0) {
            throw new IllegalArgumentException("Malfunction timeout must be positive");
        }

        this.messages = new DelayQueue<>();
        this.nodeId = ConcurrentUtils.getNextId();
        this.minDelayMillis = minDelayMillis;
        this.maxDelayMillis = maxDelayMillis;
        this.malfunctionProbability = malfunctionProbability;
        this.malfunctionTimeoutMillis = malfunctionTimeoutMillis;
        this.lastMalfunctionTime = Instant.now();
        this.state = NodeState.FOLLOWER;
        this.isActive = new AtomicBoolean(false);
        this.lock = new ReentrantLock();

        this.nodesMap = nodesMap;
        this.nodesMap.put(this.nodeId, this);
        this.thread = new Thread(getRunnable());
    }

    public static void disableLogging() {
        logger.setLevel(Level.OFF);
    }

    public static void enableLogging(Level level) {
        logger.setLevel(level);
    }

    public void sendMessage(Message message) {
        Objects.requireNonNull(message);
        messages.add(message);
    }

    public void pause() {
        if (!lock.isHeldByCurrentThread()) {
            lock.lock();
            if (logger.isLoggable(Level.INFO)) {
                logger.info(String.format("Node %d was paused", nodeId));
            }
        }
    }

    public void resume() {
        if (lock.isHeldByCurrentThread()) {
            lock.unlock();
            if (logger.isLoggable(Level.INFO)) {
                logger.info(String.format("Node %d was resumed", nodeId));
            }
        }
    }

    public void start() {
        if (!isActive.get() && !thread.isAlive()) {
            this.thread.start();
            if (logger.isLoggable(Level.INFO)) {
                logger.info(String.format("Node %d started", nodeId));
            }
        }
    }

    public void shutdown() {
        this.isActive.set(false);
        this.resume();
        if (logger.isLoggable(Level.INFO)) {
            logger.info(String.format("Shutting down node %d", nodeId));
        }
    }

    public void forceShutdown() {
        if (thread.isAlive()) {
            thread.interrupt();
        }
    }

    public long getId() {
        return this.nodeId;
    }

    public NodeState getState() {
        return this.state;
    }

    public boolean isStarted() {
        return thread.isAlive();
    }

    private Runnable getRunnable() {
        if (logger.isLoggable(Level.INFO)) {
            logger.info(String.format("Node %d initialized", nodeId));
        }
        this.messages.add(new Message(MessageType.PING, nodeId, null, this.term));

        return () -> {
            this.isActive.set(true);

            while (isActive.get()) {
                try {
                    if (lock.isLocked()) { // Block if paused
                        lock.lock();
                    }
                    Message requestMessage = messages.take();

                    switch (requestMessage.getType()) {
                        case PING -> doPingMessageLogic(requestMessage);

                        case ELECT -> doElectMessageLogic(requestMessage);

                        case ANSWER -> doAnswerMessageLogic(requestMessage);

                        case VICTORY -> doVictoryMessageLogic(requestMessage);
                    }
                    tryMalfunction();
                    Thread.sleep(100);

                } catch (InterruptedException e) {
                    logger.warning(String.format("Node %d was interrupted", nodeId));
                    return;

                } finally {
                    if (lock.isHeldByCurrentThread()) {
                        lock.unlock();
                    }
                }
            }
            if (logger.isLoggable(Level.INFO)) {
                logger.info(String.format("Node %d stopped", nodeId));
            }
        };
    }

    private void doPingMessageLogic(Message requestMessage) {
        switch (this.state) {
            case FOLLOWER -> {
                // Do ping timeout logic if message is a check from current term
                if (requestMessage.getFromNodeId() == this.nodeId && requestMessage.getSenderTerm() == this.term) {
                    if (this.leaderId == null || !this.receivedPing) {
                        if (logger.isLoggable(Level.INFO)) {
                            logger.info(String.format("Node %d becoming candidate after ping timeout", nodeId));
                        }
                        this.leaderId = null;
                        this.state = NodeState.CANDIDATE;
                        sendElectionMessage();
                        sendDelayedElectionCheck();

                        // Leader is alive, send next ping
                    } else {
                        if (logger.isLoggable(Level.FINE)) {
                            logger.info(String.format("Node %d sending next ping to leader %d", this.nodeId, this.leaderId));
                        }
                        this.receivedPing = false;
                        sendPing();
                        sendDelayedPingCheck();
                    }
                }
            }
            case LEADER -> {
                // If message is a ping from a follower, send answer
                if (requestMessage.getFromNodeId() != this.nodeId) {
                    Message responseMessage = new Message(
                            MessageType.ANSWER,
                            this.nodeId,
                            this.nodeId,
                            this.term,
                            requestMessage.getType()
                    );
                    sendMessage(responseMessage, List.of(requestMessage.getFromNodeId()));

                } else {
                    sendDelayedPingCheck();
                }
            }
            default -> {
            }
        }
    }

    private void doElectMessageLogic(Message requestMessage) {
        // If it'a an election check, become the leader
        if (this.state == NodeState.CANDIDATE && requestMessage.getFromNodeId() == this.nodeId) {
            if (logger.isLoggable(Level.INFO)) {
                logger.info(String.format("Node %d becoming leader after election response timeout", nodeId));
            }
            this.term++;
            this.state = NodeState.LEADER;
            sendVictoryMessage();
            sendDelayedPingCheck();

            // If it's an election request from a node with lower id, send answer
        } else if (requestMessage.getFromNodeId() < this.nodeId) {
            if (logger.isLoggable(Level.INFO)) {
                logger.info(String.format("Node %d sending election answer to %d",
                                nodeId,
                                requestMessage.getFromNodeId()
                        )
                );
            }
            Message responseMessage = new Message(
                    MessageType.ANSWER,
                    this.nodeId,
                    this.leaderId,
                    this.term,
                    requestMessage.getType()
            );
            sendMessage(responseMessage, requestMessage.getFromNodeId());

            if (this.state == NodeState.LEADER) {
                Message victoryMessage = new Message(
                        MessageType.VICTORY,
                        this.nodeId,
                        this.leaderId,
                        this.term
                );
                sendMessage(victoryMessage, requestMessage.getFromNodeId());
            }

        } else if (requestMessage.getFromNodeId() > this.nodeId) { // Normally not possible
            becomeFollower();
            this.term = requestMessage.getSenderTerm();
        }
    }

    private void doAnswerMessageLogic(Message requestMessage) {
        switch (this.state) {
            case FOLLOWER -> {
                // Register ping response
                if (requestMessage.getAnsweredType() == MessageType.PING
                        && leaderId != null
                        && requestMessage.getFromNodeId() == leaderId) {
                    if (logger.isLoggable(Level.FINE)) {
                        logger.fine(String.format("Node %d received ping answer from leader node %d",
                                        nodeId,
                                        requestMessage.getFromNodeId()
                                )
                        );
                    }
                    this.receivedPing = true;
                    this.term = requestMessage.getSenderTerm();
                }
            }
            case CANDIDATE -> {
                // Switch to follower if answer is a response from a node with higher id
                if (requestMessage.getAnsweredType() == MessageType.ELECT
                        && requestMessage.getSenderTerm() >= this.term
                        && requestMessage.getFromNodeId() > this.nodeId) { // Normally redundant check
                    if (logger.isLoggable(Level.INFO)) {
                        logger.info(String.format("Node %d received election answer from %d, switching to follower",
                                        nodeId,
                                        requestMessage.getFromNodeId()
                                )
                        );
                    }
                    this.term = requestMessage.getSenderTerm();
                    this.leaderId = requestMessage.getLeaderId();
                    becomeFollower();
                }
            }
            default -> {
            }
        }
    }

    private void doVictoryMessageLogic(Message requestMessage) {
        if (requestMessage.getFromNodeId() > this.nodeId) {
            if (logger.isLoggable(Level.INFO)) {
                logger.info(String.format("Node %d becoming follower after victory message from %d",
                                nodeId,
                                requestMessage.getFromNodeId()
                        )
                );
            }
            this.leaderId = requestMessage.getFromNodeId();
            this.term = requestMessage.getSenderTerm();
            this.receivedPing = true;
            becomeFollower();

            // If it's some kind of outdated election winner, make him a follower
        } else if (this.state == NodeState.LEADER) {
            if (logger.isLoggable(Level.INFO)) {
                logger.info(String.format("Node %d sending victory message to %d to make it a follower",
                                nodeId,
                                requestMessage.getFromNodeId()
                        )
                );
            }
            Message responseMessage = new Message(
                    MessageType.VICTORY,
                    this.nodeId,
                    this.nodeId,
                    this.term,
                    requestMessage.getType()
            );
            sendMessage(responseMessage, requestMessage.getFromNodeId());
        }
    }

    private void sendElectionMessage() {
        Message electMessage = new Message(
                MessageType.ELECT,
                this.nodeId,
                null,
                this.term
        );
        sendMessage(electMessage, nodesMap.tailMap(this.nodeId, false).keySet());
    }

    private void sendVictoryMessage() {
        Message victoryMessage = new Message(
                MessageType.VICTORY,
                this.nodeId,
                this.nodeId,
                this.term
        );
        sendMessage(victoryMessage, nodesMap.keySet());
    }

    private void becomeFollower() {
        this.state = NodeState.FOLLOWER;
        sendPing();
        sendDelayedPingCheck();
    }

    private void sendPing() {
        if (this.leaderId == null) {
            return;
        }
        Message ping = new Message(
                MessageType.PING,
                this.nodeId,
                null,
                this.term
        );
        sendMessage(ping, this.leaderId);
    }

    private void sendDelayedPingCheck() {
        Message delayedCheck = new Message(
                MessageType.PING,
                this.nodeId,
                null,
                this.term,
                MessageType.PING,
                getRandomDurationMillis());
        sendMessage(delayedCheck);
    }

    private void sendDelayedElectionCheck() {
        Message delayedCheck = new Message(
                MessageType.ELECT,
                this.nodeId,
                null,
                this.term,
                MessageType.ELECT,
                getRandomDurationMillis()
        );
        sendMessage(delayedCheck);
    }

    private void sendMessage(Message message, long id) {
        sendMessage(message, List.of(id));
    }

    private void sendMessage(Message message, Collection<Long> ids) {
        if (ids.isEmpty()) {
            return;
        }
        for (Long id : ids) {
            if (id == null) {
                if (logger.isLoggable(Level.WARNING)) {
                    logger.warning("Tried to send message to null node");
                }
                continue;
            }
            if (!nodesMap.containsKey(id)) {
                if (logger.isLoggable(Level.WARNING)) {
                    logger.warning(String.format("Tried to send message to non-existent node %d", id));
                }
                continue;
            }
            if (id != this.nodeId) {
                nodesMap.get(id).sendMessage(message);
            }
        }
    }

    private long getRandomDurationMillis() {
        return ThreadLocalRandom.current().nextLong(minDelayMillis, maxDelayMillis);
    }

    private void tryMalfunction() throws InterruptedException {
        if (lastMalfunctionTime.plusMillis(malfunctionTimeoutMillis).isAfter(Instant.now())) {
            return;
        }
        if (ThreadLocalRandom.current().nextInt(100) <= this.malfunctionProbability) {
            if (logger.isLoggable(Level.INFO)) {
                logger.info(String.format("Node %d is malfunctioning", nodeId));
            }
            NodeState prevState = this.state;
            this.state = NodeState.DOWN;
            Thread.sleep(ThreadLocalRandom.current().nextLong(this.maxDelayMillis, this.maxDelayMillis * 2 + 1));
            this.state = prevState;
        }
        this.lastMalfunctionTime = Instant.now();
    }
}
