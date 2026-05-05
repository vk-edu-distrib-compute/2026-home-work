package company.vk.edu.distrib.compute.luckyslon2003.consensus;

public class Message {

    public enum Type {
        PING,
        ELECT,
        ANSWER,
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
