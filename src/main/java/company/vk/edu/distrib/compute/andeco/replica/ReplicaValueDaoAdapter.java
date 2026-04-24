package company.vk.edu.distrib.compute.andeco.replica;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Base64;

public class ReplicaValueDaoAdapter implements Dao<ReplicaValue> {

    private final Dao<byte[]> delegate;

    public ReplicaValueDaoAdapter(Dao<byte[]> delegate) {
        this.delegate = delegate;
    }

    @Override
    public ReplicaValue get(String key) throws IOException {
        byte[] raw = delegate.get(key);

        String content = new String(raw);

        String[] parts = content.split("\\|", 3);

        BigInteger version = new BigInteger(parts[0]);
        boolean deleted = Boolean.parseBoolean(parts[1]);

        byte[] value = "null".equals(parts[2])
                ? null
                : Base64.getDecoder().decode(parts[2]);

        return new ReplicaValue(value, version, deleted);
    }

    @Override
    public void upsert(String key, ReplicaValue value) throws IOException {
        String encodedValue = value.value() == null
                ? "null"
                : Base64.getEncoder().encodeToString(value.value());

        String content = value.version()
                + "|"
                + value.deleted()
                + "|"
                + encodedValue;

        delegate.upsert(key, content.getBytes());
    }

    @Override
    public void delete(String key) throws IOException {
        delegate.delete(key);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
