package company.vk.edu.distrib.compute.andeco.replica;

import company.vk.edu.distrib.compute.andeco.FileDao;

import java.io.IOException;
import java.math.BigInteger;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

public class Replica {
    private final long id;
    private boolean available = true;
    private final FileDao fileDao;
    private final ReplicaValueDaoAdapter data;

    private final AtomicLong readCount = new AtomicLong();
    private final AtomicLong writeCount = new AtomicLong();

    public Replica(long id, FileDao fileDao) {
        this.id = id;
        this.fileDao = fileDao;
        this.data = new ReplicaValueDaoAdapter(fileDao);
    }

    public long getId() {
        return id;
    }

    public boolean isAvailable() {
        return available;
    }

    public void setAvailable(boolean available) {
        this.available = available;
    }

    public void write(String key, byte[] value, BigInteger version) throws IOException {
        writeCount.incrementAndGet();
        data.upsert(key, new ReplicaValue(value, version, false));
    }

    public ReplicaValue read(String key) throws IOException {
        readCount.incrementAndGet();
        try {
            return data.get(key);
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    public void delete(String key, BigInteger version) throws IOException {
        writeCount.incrementAndGet();
        data.upsert(key, new ReplicaValue(null, version, true));
    }

    public int keysCount() throws IOException {
        return fileDao.size();
    }

    public long readAccessCount() {
        return readCount.get();
    }

    public long writeAccessCount() {
        return writeCount.get();
    }
}
