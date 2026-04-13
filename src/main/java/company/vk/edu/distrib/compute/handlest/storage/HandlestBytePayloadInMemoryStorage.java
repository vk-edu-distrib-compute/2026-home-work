package company.vk.edu.distrib.compute.handlest.storage;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class HandlestBytePayloadInMemoryStorage implements HandlestStorage<byte[]> {
    private final ConcurrentMap<String, byte[]> storage = new ConcurrentHashMap<>();

    @Override
    public byte[] get(String key) {
        return storage.get(key);
    }

    @Override
    public void put(String key, byte[] value) {
        storage.put(key, value);
    }

    @Override
    public void remove(String key) {
        storage.remove(key);
    }

    @Override
    public void clear() {
        storage.clear();
    }
}
