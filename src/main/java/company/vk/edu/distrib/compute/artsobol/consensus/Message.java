package company.vk.edu.distrib.compute.artsobol.consensus;

record Message(
        MessageType type,
        int senderId,
        int requestId,
        MessageType responseTo,
        int leaderId
) {
    static Message ping(int senderId, int requestId) {
        return new Message(MessageType.PING, senderId, requestId, null, ConsensusCluster.NO_LEADER);
    }

    static Message elect(int senderId, int requestId) {
        return new Message(MessageType.ELECT, senderId, requestId, null, ConsensusCluster.NO_LEADER);
    }

    static Message answer(int senderId, int requestId, MessageType responseTo) {
        return new Message(MessageType.ANSWER, senderId, requestId, responseTo, ConsensusCluster.NO_LEADER);
    }

    static Message victory(int senderId) {
        return new Message(MessageType.VICTORY, senderId, 0, null, senderId);
    }
}
