package company.vk.edu.distrib.compute.artttnik;

public class Message {
    public enum Type {
        PING, ELECT, ANSWER, VICTORY
    }

    public final Type type;
    public final int fromId;
    public final Integer leaderId;

    public Message(Type type, int fromId, Integer leaderId) {
        this.type = type;
        this.fromId = fromId;
        this.leaderId = leaderId;
    }

    public static Message ping(int fromId) {
        return new Message(Type.PING, fromId, null);
    }

    public static Message elect(int fromId) {
        return new Message(Type.ELECT, fromId, null);
    }

    public static Message answer(int fromId) {
        return new Message(Type.ANSWER, fromId, null);
    }

    public static Message victory(int fromId, int leaderId) {
        return new Message(Type.VICTORY, fromId, leaderId);
    }
}
