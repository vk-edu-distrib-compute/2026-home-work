package company.vk.edu.distrib.compute.maryarta.consensus;

public class Message {
    MessageType type;
    int fromId;
    int toId;
    public MessageType answerTo;

    public Message(MessageType type, int fromId, int toId, MessageType answerTo) {
        this.type = type;
        this.fromId = fromId;
        this.toId = toId;
        this.answerTo = answerTo;
    }
}
