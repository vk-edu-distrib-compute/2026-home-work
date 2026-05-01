package company.vk.edu.distrib.compute.vitos23.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Node {

    private static final Logger log = LoggerFactory.getLogger(Node.class);
    private static final int NO_LEADER = -1;

    private final int id;
    private final MessageSender messageSender;
    private final BlockingQueue<Message> messageQueue = new ArrayBlockingQueue<>(10000);
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final ReentrantLock electionLock = new ReentrantLock();
    private final Condition electionCondition = electionLock.newCondition();

    private final AtomicInteger leaderId = new AtomicInteger(NO_LEADER);
    private final AtomicBoolean leaderConfirmed = new AtomicBoolean(false);
    private final AtomicBoolean broken = new AtomicBoolean(false);

    private boolean electing;
    private boolean receivedAnswerToElect;

    public Node(int id, MessageSender messageSender) {
        this.id = id;
        this.messageSender = messageSender;
    }

    public int getId() {
        return id;
    }

    public Integer getLeaderId() {
        int localLeaderId = leaderId.get();
        return localLeaderId == NO_LEADER ? null : localLeaderId;
    }

    public boolean isBroken() {
        return broken.get();
    }

    public NodeStatus getStatus() {
        if (broken.get()) {
            return NodeStatus.DOWN;
        }
        return leaderId.get() == id ? NodeStatus.LEADER : NodeStatus.FOLLOWER;
    }

    public void setBroken(boolean broken) {
        boolean wasBroken = this.broken.getAndSet(broken);
        if (wasBroken && !broken) {
            log.info("Node {} recovered, initiating election", id);
            initiateElection();
        }
    }

    public void addMessageToQueue(Message message) {
        boolean added = messageQueue.add(message);
        if (!added) {
            log.warn("Node {}: queue overflow, message discarded", id);
        }
    }

    public void start() throws InterruptedException {
        initiateElection();
        executor.submit(this::startLeaderPing);
        while (!Thread.currentThread().isInterrupted()) {
            Message message = messageQueue.take();
            if (broken.get()) {
                // Discarding message if we are broken
                continue;
            }
            switch (message.type()) {
                case PING -> messageSender.send(message.fromId(), new Message(id, MessageType.ANSWER));
                case ELECT -> {
                    messageSender.send(message.fromId(), new Message(id, MessageType.ANSWER));
                    initiateElection();
                }
                case ANSWER -> {
                    boolean answeredToElect = false;
                    electionLock.lock();
                    try {
                        if (electing) {
                            receivedAnswerToElect = true;
                            electionCondition.signalAll();
                            answeredToElect = true;
                        }
                    } finally {
                        electionLock.unlock();
                    }
                    if (!answeredToElect && leaderId.get() != NO_LEADER && leaderId.get() == message.fromId()) {
                        leaderConfirmed.set(true);
                    }
                }
                case VICTORY -> {
                    electionLock.lock();
                    try {
                        electing = false;
                        electionCondition.signalAll();
                    } finally {
                        electionLock.unlock();
                    }
                    leaderId.set(message.fromId());
                    leaderConfirmed.set(true);
                }
            }
        }
    }

    // False positive
    @SuppressWarnings("PMD.UnusedAssignment")
    void initiateElection() {
        executor.submit(() -> {
            electionLock.lock();
            try {
                if (electing) {
                    return;
                }
                electing = true;
                receivedAnswerToElect = false;
            } finally {
                electionLock.unlock();
            }

            log.info("Node {} initiated election", id);
            if (!broken.get()) {
                messageSender.sendToMoreImportant(new Message(id, MessageType.ELECT));
            }

            electionLock.lock();
            try {
                electionCondition.await(SimulationProperties.ELECTION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

                if (!receivedAnswerToElect && !broken.get()) {
                    leaderId.set(id);
                    leaderConfirmed.set(true);
                    log.info("Node {} became leader", id);
                    messageSender.sendToAll(new Message(id, MessageType.VICTORY));
                }
                electing = false;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                electing = false;
            } finally {
                electionLock.unlock();
            }
        });
    }

    private void startLeaderPing() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                int localLeaderId = leaderId.get();
                if (localLeaderId == NO_LEADER || localLeaderId == id || broken.get()) {
                    Thread.sleep(SimulationProperties.PING_INTERVAL.toMillis());
                    continue;
                }

                leaderConfirmed.set(false);
                log.info("Node {}: pinging leader {}", id, localLeaderId);
                messageSender.send(localLeaderId, new Message(id, MessageType.PING));

                Thread.sleep(SimulationProperties.REQUEST_TIMEOUT.toMillis());

                if (localLeaderId != leaderId.get()) {
                    // Leader changed, new ping is required
                    continue;
                }
                if (leaderConfirmed.get()) {
                    log.info("Node {}: confirmed leader {}", id, localLeaderId);
                } else {
                    log.info("Node {}: leader {} might be broken", id, localLeaderId);
                    leaderId.set(NO_LEADER);
                    initiateElection();
                }

                Thread.sleep(SimulationProperties.PING_INTERVAL.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

}
