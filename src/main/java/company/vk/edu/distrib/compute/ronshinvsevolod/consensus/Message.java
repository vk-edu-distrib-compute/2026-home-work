package company.vk.edu.distrib.compute.ronshinvsevolod.consensus;

public final class Message {
    private final MessageType type;
    private final int senderId;

    public Message(final MessageType type, final int senderId) {
        this.type = type;
        this.senderId = senderId;
    }

    public MessageType getType() {
        return type;
    }

    public int getSenderId() {
        return senderId;
    }
}
