package company.vk.edu.distrib.compute.tadzhnahal;

import java.io.IOException;
import java.nio.ByteBuffer;

public final class TadzhnahalReplicaRecordCodec {
    private static final int LONG_BYTES = Long.BYTES;
    private static final int INT_BYTES = Integer.BYTES;
    private static final int BOOLEAN_BYTES = 1;
    private static final int HEADER_SIZE = LONG_BYTES + BOOLEAN_BYTES + INT_BYTES;

    public byte[] encode(TadzhnahalReplicaRecord record) {
        byte[] value = record.value();
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE + value.length);
        buffer.putLong(record.version());
        buffer.put((byte) (record.deleted() ? 1 : 0));
        buffer.putInt(value.length);
        buffer.put(value);
        return buffer.array();
    }

    public TadzhnahalReplicaRecord decode(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length < HEADER_SIZE) {
            throw new IOException("Invalid replica record");
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long version = buffer.getLong();
        boolean deleted = buffer.get() != 0;
        int valueLength = buffer.getInt();

        if (valueLength < 0 || bytes.length != HEADER_SIZE + valueLength) {
            throw new IOException("Invalid replica record length");
        }

        byte[] value = new byte[valueLength];
        buffer.get(value);

        return new TadzhnahalReplicaRecord(value, version, deleted);
    }
}
