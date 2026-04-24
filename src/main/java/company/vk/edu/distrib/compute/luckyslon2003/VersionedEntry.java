package company.vk.edu.distrib.compute.luckyslon2003;

import java.nio.ByteBuffer;

final class VersionedEntry {
    private static final int HEADER_SIZE = Long.BYTES + 1 + Integer.BYTES;

    private final long version;
    private final boolean tombstone;
    private final byte[] value;

    VersionedEntry(long version, boolean tombstone, byte[] value) {
        this.version = version;
        this.tombstone = tombstone;
        this.value = value == null ? null : value.clone();
    }

    long version() {
        return version;
    }

    boolean tombstone() {
        return tombstone;
    }

    byte[] value() {
        return value == null ? null : value.clone();
    }

    int valueSize() {
        return value == null ? 0 : value.length;
    }

    byte[] serialize() {
        int valueLength = value == null ? 0 : value.length;
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE + valueLength);
        buffer.putLong(version);
        buffer.put((byte) (tombstone ? 1 : 0));
        buffer.putInt(valueLength);
        if (valueLength > 0) {
            buffer.put(value);
        }
        return buffer.array();
    }

    static VersionedEntry deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long version = buffer.getLong();
        boolean tombstone = buffer.get() != 0;
        int valueLength = buffer.getInt();
        byte[] value = null;
        if (!tombstone) {
            value = new byte[valueLength];
        }
        if (valueLength > 0) {
            buffer.get(value);
        }
        return new VersionedEntry(version, tombstone, value);
    }
}
