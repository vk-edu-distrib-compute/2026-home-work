package company.vk.edu.distrib.compute.tadzhnahal.consensus;

public class Message {
    private final MessageType type;
    private final int fromId;
    private final int toId;
    private final int leaderId;

    public Message(MessageType type, int fromId, int toId, int leaderId) {
        if (type == null) {
            throw new IllegalArgumentException("message type is null");
        }

        this.type = type;
        this.fromId = fromId;
        this.toId = toId;
        this.leaderId = leaderId;
    }

    public MessageType getType() {
        return type;
    }

    public int getFromId() {
        return fromId;
    }

    public int getToId() {
        return toId;
    }

    public int getLeaderId() {
        return leaderId;
    }

    @Override
    public String toString() {
        return "Message{"
                + "type=" + type
                + ", fromId=" + fromId
                + ", toId=" + toId
                + ", leaderId=" + leaderId
                + '}';
    }
}
