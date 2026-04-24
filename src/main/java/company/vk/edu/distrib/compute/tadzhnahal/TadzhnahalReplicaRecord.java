package company.vk.edu.distrib.compute.tadzhnahal;

import java.util.Arrays;

public record TadzhnahalReplicaRecord(byte[] value, long version, boolean deleted) {
    public TadzhnahalReplicaRecord {
        if (value == null) {
            value = new byte[0];
        } else {
            value = Arrays.copyOf(value, value.length);
        }
    }

    @Override
    public byte[] value() {
        return Arrays.copyOf(value, value.length);
    }
}
