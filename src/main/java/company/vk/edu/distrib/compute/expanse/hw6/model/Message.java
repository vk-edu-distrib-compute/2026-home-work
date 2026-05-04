package company.vk.edu.distrib.compute.expanse.hw6.model;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class Message implements Delayed {
    private final MessageType type;
    private final long fromNodeId;
    private final Long leaderId;
    private final MessageType answeredType;
    private final long delayMillis;
    private final long senderTerm;

    public Message(MessageType type, long fromNodeId, Long leaderId, long senderTerm, MessageType answeredType, long delayMillis) {
        this.type = type;
        this.fromNodeId = fromNodeId;
        this.leaderId = leaderId;
        this.senderTerm = senderTerm;
        this.answeredType = answeredType;
        this.delayMillis = delayMillis;
    }

    public Message(MessageType type, long fromNodeId, Long leaderId, long senderTerm, MessageType answeredType) {
        this.type = type;
        this.fromNodeId = fromNodeId;
        this.leaderId = leaderId;
        this.senderTerm = senderTerm;
        this.answeredType = answeredType;
        this.delayMillis = 0;
    }

    public Message(MessageType type, long fromNodeId, Long leaderId, long senderTerm) {
        this.type = type;
        this.fromNodeId = fromNodeId;
        this.leaderId = leaderId;
        this.senderTerm = senderTerm;
        this.answeredType = null;
        this.delayMillis = 0;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return TimeUnit.MILLISECONDS.convert(delayMillis, unit);
    }

    @Override
    public int compareTo(Delayed o) {
        return Long.compare(getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
    }

    public MessageType getType() {
        return type;
    }

    public long getFromNodeId() {
        return fromNodeId;
    }

    public Long getLeaderId() {
        return leaderId;
    }

    public long getSenderTerm() {
        return senderTerm;
    }

    public MessageType getAnsweredType() {
        return answeredType;
    }

    public long getDelayMillis() {
        return delayMillis;
    }
}
