package company.vk.edu.distrib.compute.dariaprindina.consensus;

import java.util.Objects;

public record ConsensusMessage(
    ConsensusMessageType type,
    int senderId
) {
    public ConsensusMessage {
        Objects.requireNonNull(type, "type");
    }
}
