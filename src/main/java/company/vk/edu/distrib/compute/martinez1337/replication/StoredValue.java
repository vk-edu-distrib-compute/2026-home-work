package company.vk.edu.distrib.compute.martinez1337.replication;

import java.nio.ByteBuffer;

record StoredValue(long version, boolean tombstone, byte[] value) {
    static StoredValue value(byte[] value, long version) {
        return new StoredValue(version, false, value);
    }

    static StoredValue tombstone(long version) {
        return new StoredValue(version, true, null);
    }

    byte[] encode() {
        int payloadLength = value == null ? 0 : value.length;
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + 1 + Integer.BYTES + payloadLength);
        buffer.putLong(version);
        buffer.put((byte) (tombstone ? 1 : 0));
        buffer.putInt(payloadLength);
        if (payloadLength > 0) {
            buffer.put(value);
        }
        return buffer.array();
    }

    static StoredValue decode(byte[] raw) {
        if (raw == null || raw.length < Long.BYTES + 1 + Integer.BYTES) {
            throw new IllegalArgumentException("Invalid stored value: too short");
        }
        ByteBuffer buffer = ByteBuffer.wrap(raw);
        long version = buffer.getLong();
        boolean tombstone = buffer.get() == 1;
        int payloadLength = buffer.getInt();
        if (payloadLength < 0 || buffer.remaining() < payloadLength) {
            throw new IllegalArgumentException("Invalid stored value: broken payload length");
        }
        byte[] value = null;
        if (payloadLength > 0) {
            value = new byte[payloadLength];
            buffer.get(value);
        } else if (!tombstone) {
            value = new byte[0];
        }
        return new StoredValue(version, tombstone, value);
    }
}
