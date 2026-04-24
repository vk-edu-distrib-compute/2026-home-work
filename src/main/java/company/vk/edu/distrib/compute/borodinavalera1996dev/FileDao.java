package company.vk.edu.distrib.compute.borodinavalera1996dev;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.file.Path;
import java.util.NoSuchElementException;

public class FileDao implements Dao<FileStorage.Data> {

    private final FileStorage storage;

    public FileDao(Path path) {
        this.storage = new FileStorage(path);
    }

    @Override
    public FileStorage.Data get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        return storage.readFromFile(key);
    }

    @Override
    public void upsert(String key, FileStorage.Data value) throws IllegalArgumentException, IOException {
        storage.save(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        storage.deleteFromFile(key);
    }

    @Override
    public void close() throws IOException {
        //nope
    }
}
