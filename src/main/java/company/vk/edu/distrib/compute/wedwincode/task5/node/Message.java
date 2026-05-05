package company.vk.edu.distrib.compute.wedwincode.task5.node;

public record Message(Type type, int senderId) {
    public enum Type {
        PING, ELECT, ANSWER, VICTORY
    }
}
