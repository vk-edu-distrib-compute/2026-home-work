package company.vk.edu.distrib.compute.luckyslon2003.consensus;

/**
 * Represents a message exchanged between cluster nodes.
 */
public class Message {

    public enum Type {
        /** Sent by followers to the leader to check availability. */
        PING,
        /** Sent to nodes with higher IDs to initiate an election. */
        ELECT,
        /** Response to PING or ELECT — "I'm alive". */
        ANSWER,
        /** Broadcast by the new leader to announce victory. */
        VICTORY
    }

    private final Type type;
    private final int senderId;
    private final long timestamp;

    public Message(Type type, int senderId) {
        this.type = type;
        this.senderId = senderId;
        this.timestamp = System.currentTimeMillis();
    }

    public Type getType() {
        return type;
    }

    public int getSenderId() {
        return senderId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "Message{type=" + type + ", senderId=" + senderId + ", ts=" + timestamp + "}";
    }
}
