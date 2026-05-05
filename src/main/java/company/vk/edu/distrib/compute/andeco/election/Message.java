package company.vk.edu.distrib.compute.andeco.election;

import java.util.Objects;

public final class Message {
    private final MessageType type;
    private final int from;
    private final long election;

    public Message(MessageType type, int from, long election) {
        this.type = Objects.requireNonNull(type);
        this.from = from;
        this.election = election;
    }

    public MessageType type() {
        return type;
    }

    public int from() {
        return from;
    }

    public long election() {
        return election;
    }
}

