package company.vk.edu.distrib.compute.vitos23.consensus;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class MessageSender {

    private final Map<Integer, Node> nodeById;

    public MessageSender(Map<Integer, Node> nodeById) {
        this.nodeById = nodeById;
    }

    public void send(int target, Message message) {
        requireNonNull(nodeById.get(target)).addMessageToQueue(message);
    }

    public void sendToMoreImportant(Message message) {
        nodeById.entrySet().stream()
                .filter(entry -> entry.getKey() > message.fromId())
                .forEach(entry -> entry.getValue().addMessageToQueue(message));
    }

    public void sendToAll(Message message) {
        nodeById.values().forEach(node -> node.addMessageToQueue(message));
    }

}
