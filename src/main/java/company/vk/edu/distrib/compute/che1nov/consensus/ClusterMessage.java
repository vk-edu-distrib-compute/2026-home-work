package company.vk.edu.distrib.compute.che1nov.consensus;

public record ClusterMessage(
        MessageType type,
        int fromId,
        AnswerKind answerKind
) {
    public static ClusterMessage ping(int fromId) {
        return new ClusterMessage(MessageType.PING, fromId, null);
    }

    public static ClusterMessage elect(int fromId) {
        return new ClusterMessage(MessageType.ELECT, fromId, null);
    }

    public static ClusterMessage answer(int fromId, AnswerKind answerKind) {
        return new ClusterMessage(MessageType.ANSWER, fromId, answerKind);
    }

    public static ClusterMessage victory(int fromId) {
        return new ClusterMessage(MessageType.VICTORY, fromId, null);
    }
}
