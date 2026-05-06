package company.vk.edu.distrib.compute.aldor7705.hw5;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class Message implements Delayed {
    private final MessageType type;
    private final int senderId;
    private final long timestamp;
    private final long delayMillis;
    private final MessageType inResponseTo;

    public Message(MessageType type, int senderId) {
        this(type, senderId, 0, null);
    }

    public Message(MessageType type, int senderId, long delayMillis, MessageType inResponseTo) {
        this.type = type;
        this.senderId = senderId;
        this.timestamp = System.currentTimeMillis();
        this.delayMillis = delayMillis;
        this.inResponseTo = inResponseTo;
    }

    public MessageType getType() {
        return type;
    }

    public int getSenderId() {
        return senderId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public MessageType getInResponseTo() {
        return inResponseTo;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        long remaining = delayMillis - (System.currentTimeMillis() - timestamp);
        return unit.convert(Math.max(0, remaining), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        return Long.compare(this.getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
    }

    @Override
    public String toString() {
        return String.format("Сообщение{тип=%s, от=%d%s%s}",
                type, senderId,
                inResponseTo != null ? ", ответ_на=" + inResponseTo : "",
                delayMillis > 0 ? ", задержка=" + delayMillis + "мс" : "");
    }
}
