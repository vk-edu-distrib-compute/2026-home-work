package company.vk.edu.distrib.compute.vladislavguzov;

import java.nio.ByteBuffer;

/**
 * Format: [8 bytes: long timestamp][1 byte: type][data...]
 * type = 0x00 — tombstone (deletion marker), type = 0x01 — regular value.
 */
record VersionedValue(long timestamp, byte[] data) {

    private static final byte TYPE_TOMBSTONE = 0x00;
    private static final byte TYPE_DATA = 0x01;
    static final int HEADER_SIZE = 9;

    boolean isDeleted() {
        return data == null;
    }

    static byte[] encode(long timestamp, byte[] data) {
        if (data == null) {
            return ByteBuffer.allocate(HEADER_SIZE)
                    .putLong(timestamp)
                    .put(TYPE_TOMBSTONE)
                    .array();
        }
        return ByteBuffer.allocate(HEADER_SIZE + data.length)
                .putLong(timestamp)
                .put(TYPE_DATA)
                .put(data)
                .array();
    }

    static VersionedValue decode(byte[] raw) {
        ByteBuffer buf = ByteBuffer.wrap(raw);
        long timestamp = buf.getLong();
        byte type = buf.get();
        if (type == TYPE_TOMBSTONE) {
            return new VersionedValue(timestamp, null);
        }
        byte[] value = new byte[buf.remaining()];
        buf.get(value);
        return new VersionedValue(timestamp, value);
    }
}
