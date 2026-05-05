package company.vk.edu.distrib.compute.nst1610.consensus;

public record Message(
    MessageType type,
    int senderId,
    long electionClock,
    int leaderId,
    ResponseKind responseKind
) {
    public static Message ping(int senderId, long electionClock, int leaderId) {
        return new Message(MessageType.PING, senderId, electionClock, leaderId, null);
    }

    public static Message elect(int senderId, long electionClock) {
        return new Message(MessageType.ELECT, senderId, electionClock, -1, null);
    }

    public static Message answer(int senderId, long electionClock, ResponseKind responseKind) {
        return new Message(MessageType.ANSWER, senderId, electionClock, -1, responseKind);
    }

    public static Message victory(int senderId, long electionClock) {
        return new Message(MessageType.VICTORY, senderId, electionClock, senderId, null);
    }
}
