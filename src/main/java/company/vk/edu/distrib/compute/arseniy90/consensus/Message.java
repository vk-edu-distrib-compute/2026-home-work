
package company.vk.edu.distrib.compute.arseniy90.consensus;

public class Message {
    private final MessageType type;
    private final int senderId;
    private final String payload;

    public Message(MessageType type, int senderId, String payload) {
        this.type = type;
        this.senderId = senderId;
        this.payload = payload;
    }

    public MessageType getType() {
        return type;
    }

    public int getSenderId() {
        return senderId;
    }

    public String getPayload() {
        return payload;
    }
}
