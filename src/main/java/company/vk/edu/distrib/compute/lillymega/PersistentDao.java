package company.vk.edu.distrib.compute.lillymega;

import company.vk.edu.distrib.compute.Dao;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import java.nio.file.Path;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class PersistentDao implements Dao<byte[]> {
    private final Map<String, byte[]> storage = new ConcurrentHashMap<>();
    private final Path file;

    public PersistentDao(Path file) throws IOException {
        this.file = file;
        if (file.getParent() != null) {
            Files.createDirectories(file.getParent());
        }
        loadFromFile();
    }

    private static void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key must be not empty");
        }
    }

    @Override
    public byte[] get(String key) {
        validateKey(key);
        byte[] value = storage.get(key);
        if (value == null) {
            throw new NoSuchElementException("No value for key: " + key);
        }
        return value;
    }

    @Override
    public void upsert(String key, byte[] value) throws IOException {
        validateKey(key);
        if (value == null) {
            throw new IllegalArgumentException("Value must be not null");
        }

        storage.put(key, value);
        saveToFile();
    }

    @Override
    public void delete(String key) throws IOException {
        validateKey(key);
        storage.remove(key);
        saveToFile();
    }

    @Override
    public void close() throws IOException {
        saveToFile();
    }

    private void loadFromFile() throws IOException {
        if (!Files.exists(file)) {
            return;
        }

        try (InputStream is = Files.newInputStream(file);
             BufferedInputStream bis = new BufferedInputStream(is);
             DataInputStream dis = new DataInputStream(bis)) {

            while (true) {
                try {
                    int keyLength = dis.readInt();
                    byte[] keyBytes = new byte[keyLength];
                    dis.readFully(keyBytes);

                    int valueLength = dis.readInt();
                    byte[] valueBytes = new byte[valueLength];
                    dis.readFully(valueBytes);

                    String key = new String(keyBytes, StandardCharsets.UTF_8);
                    storage.put(key, valueBytes);
                } catch (EOFException e) {
                    break;
                }
            }
        }
    }

    private void saveToFile() throws IOException {
        try (OutputStream os = Files.newOutputStream(file);
             BufferedOutputStream bos = new BufferedOutputStream(os);
             DataOutputStream dos = new DataOutputStream(bos)) {

            for (Map.Entry<String, byte[]> entry : storage.entrySet()) {
                byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
                byte[] valueBytes = entry.getValue();

                dos.writeInt(keyBytes.length);
                dos.write(keyBytes);

                dos.writeInt(valueBytes.length);
                dos.write(valueBytes);
            }
        }
    }

}
