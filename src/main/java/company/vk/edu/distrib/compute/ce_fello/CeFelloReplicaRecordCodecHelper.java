package company.vk.edu.distrib.compute.ce_fello;

import java.nio.ByteBuffer;

final class CeFelloReplicaRecordCodecHelper {
    private static final int TOMBSTONE_FLAG = 1;
    private static final int LIVE_FLAG = 0;

    private CeFelloReplicaRecordCodecHelper() {
    }

    static byte[] encode(CeFelloReplicaRecord record) {
        byte[] value = record.value();
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + Integer.BYTES + Integer.BYTES + value.length);
        buffer.putLong(record.version());
        buffer.putInt(record.tombstone() ? TOMBSTONE_FLAG : LIVE_FLAG);
        buffer.putInt(value.length);
        buffer.put(value);
        return buffer.array();
    }

    static CeFelloReplicaRecord decode(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        long version = buffer.getLong();
        boolean tombstone = buffer.getInt() == TOMBSTONE_FLAG;
        int valueLength = buffer.getInt();
        if (valueLength < 0 || valueLength > buffer.remaining()) {
            throw new IllegalArgumentException("Invalid record payload");
        }

        byte[] value = new byte[valueLength];
        buffer.get(value);
        return new CeFelloReplicaRecord(version, tombstone, value);
    }
}
