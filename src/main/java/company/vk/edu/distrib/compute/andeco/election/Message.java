package company.vk.edu.distrib.compute.andeco.election;

import java.util.Objects;

public final class Message {
    private final MessageType type;
    private final int from;
    private final long electionId;

    public Message(MessageType type, int from, long electionId) {
        this.type = Objects.requireNonNull(type);
        this.from = from;
        this.electionId = electionId;
    }

    public MessageType getType() {
        return type;
    }

    public int from() {
        return from;
    }

    public long getElectionId() {
        return electionId;
    }
}

