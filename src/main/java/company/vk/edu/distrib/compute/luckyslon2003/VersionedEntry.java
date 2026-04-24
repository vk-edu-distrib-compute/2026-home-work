package company.vk.edu.distrib.compute.luckyslon2003;

import java.nio.ByteBuffer;

final class VersionedEntry {
    private static final int HEADER_SIZE = Long.BYTES + 1 + Integer.BYTES;

    private final long entryVersion;
    private final boolean tombstoneMarker;
    private final byte[] entryValue;

    VersionedEntry(long version, boolean tombstone, byte[] value) {
        this.entryVersion = version;
        this.tombstoneMarker = tombstone;
        this.entryValue = value == null ? null : value.clone();
    }

    long version() {
        return entryVersion;
    }

    boolean tombstone() {
        return tombstoneMarker;
    }

    byte[] value() {
        return entryValue == null ? null : entryValue.clone();
    }

    int valueSize() {
        return entryValue == null ? 0 : entryValue.length;
    }

    byte[] serialize() {
        int valueLength = entryValue == null ? 0 : entryValue.length;
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE + valueLength);
        buffer.putLong(entryVersion);
        buffer.put((byte) (tombstoneMarker ? 1 : 0));
        buffer.putInt(valueLength);
        if (valueLength > 0) {
            buffer.put(entryValue);
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
