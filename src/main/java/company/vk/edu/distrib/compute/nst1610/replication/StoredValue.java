package company.vk.edu.distrib.compute.nst1610.replication;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public record StoredValue(long timestamp, boolean tombstone, byte[] value) {
    public byte[] encode() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (DataOutputStream dataOutput = new DataOutputStream(output)) {
            dataOutput.writeLong(timestamp);
            dataOutput.writeBoolean(tombstone);
            if (!tombstone) {
                dataOutput.writeInt(value.length);
                dataOutput.write(value);
            }
        }
        return output.toByteArray();
    }

    public static StoredValue decode(byte[] rawValue) throws IOException {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(rawValue))) {
            long timestamp = input.readLong();
            boolean tombstone = input.readBoolean();
            if (tombstone) {
                return new StoredValue(timestamp, true, new byte[0]);
            }
            int length = input.readInt();
            byte[] value = input.readNBytes(length);
            return new StoredValue(timestamp, false, value);
        }
    }
}
