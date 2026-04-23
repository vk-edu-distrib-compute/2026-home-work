package company.vk.edu.distrib.compute.nihuaway00.storage;

import java.nio.ByteBuffer;
import java.time.Instant;

public class VersionedEntry {
    private long timestamp;
    private boolean tombstone;
    private byte[] data;

    public VersionedEntry(byte[] data) {
        actualizeTimestamp();
        this.tombstone = false;
        this.data = data == null ? new byte[0] : data.clone();
    }

    private VersionedEntry(long timestamp, boolean tombstone, byte[] data) {
        this.timestamp = timestamp;
        this.tombstone = tombstone;
        this.data = data == null ? new byte[0] : data.clone();
    }

    public byte[] serialize() {
        byte[] payload = tombstone ? new byte[0] : data;
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES + 1 + payload.length);
        buf.putLong(timestamp);
        buf.put(tombstone ? (byte) 1 : (byte) 0);
        buf.put(payload);
        return buf.array();
    }

    static VersionedEntry parse(byte[] raw) {
        ByteBuffer buf = ByteBuffer.wrap(raw);
        long timestamp = buf.getLong();
        boolean tombstone = buf.get() == 1;
        byte[] data = new byte[buf.remaining()];
        buf.get(data);
        return new VersionedEntry(timestamp, tombstone, tombstone ? new byte[0] : data);
    }

    public static VersionedEntry getAbsentInstance() {
        return new VersionedEntry(0L, true, new byte[0]);
    }

    public long getTimestamp() {
        return timestamp;
    }

    private void actualizeTimestamp() {
        this.timestamp = Instant.now().toEpochMilli();
    }

    public boolean isTombstone() {
        return tombstone;
    }

    public void setTombstone() {
        actualizeTimestamp();
        this.tombstone = true;
    }

    public byte[] getData() {
        return data == null ? null : data.clone();
    }

    public void setData(byte[] data) {
        if (data == null) {
            throw new IllegalArgumentException("data cannot be null");
        }
        actualizeTimestamp();
        this.data = data.clone();
    }
}
