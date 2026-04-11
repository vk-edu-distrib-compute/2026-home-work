package company.vk.edu.distrib.compute.handlest;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.handlest.storage.HandlestBytePayloadFileSystemStorage;
import company.vk.edu.distrib.compute.handlest.storage.HandlestStorage;

import java.io.IOException;
import java.util.NoSuchElementException;

public class HandlestDao implements Dao<byte[]> {
    private final HandlestStorage<byte[]> storage;

    public HandlestDao(String storageFolderPath) {
         this.storage = new HandlestBytePayloadFileSystemStorage(storageFolderPath);
         // this.storage = new HandlestBytePayloadInMemoryStorage();
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }

        byte[] value = storage.get(key);
        if (value == null) {
            throw new NoSuchElementException("Key not found: " + key);
        }

        return value;
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }

        storage.put(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }

        storage.remove(key);
    }

    @Override
    public void close() throws IOException {
        storage.clear();
    }
}
