package company.vk.edu.distrib.compute.nihuaway00.storage;

import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.NoSuchElementException;

public class EntityDao implements Dao<byte[]> {
    private static final Logger log = LoggerFactory.getLogger(EntityDao.class);
    private final Path baseDir;

    public EntityDao(Path baseDir) {
        this.baseDir = baseDir;
    }

    public static EntityDao create(Path path) throws IOException {
        Files.createDirectories(path);
        return new EntityDao(path);
    }

    public static EntityDao createReplica(int port, int index) throws IOException {
        return EntityDao.create(Path.of("./storage/" + port + "/replica-" + index));
    }

    public boolean available() {
        try {
            Path probe = baseDir.resolve(".probe");
            Files.write(probe, new byte[]{});
            Files.delete(probe);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
        public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException();
        }

        try {
            byte[] data = Files.readAllBytes(baseDir.resolve(key));
            VersionedEntry versioned = VersionedEntry.parse(data);

            if (versioned.isTombstone()) {
                throw new NoSuchElementException("Entity is not found in storage");
            }

            return versioned.getData();
        } catch (NoSuchFileException e) {
            throw new NoSuchElementException("Entity is not found in storage", e);
        }
    }

    public VersionedEntry getVersioned(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException();
        }

        try {
            byte[] data = Files.readAllBytes(baseDir.resolve(key));
            return VersionedEntry.parse(data);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException();
        }

        VersionedEntry versionedEntry = new VersionedEntry(value);
        Files.write(baseDir.resolve(key), versionedEntry.serialize());
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        VersionedEntry versionedEntry = getVersioned(key);

        if(versionedEntry == null){
            versionedEntry = new VersionedEntry(new byte[0]);
        }

        try {
            versionedEntry.setTombstone();
            Files.write(baseDir.resolve(key), versionedEntry.serialize());
        } catch (NoSuchFileException e) {
            if (log.isWarnEnabled()) {
                log.warn("Entity is not found in storage. Nothing to delete", e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        // он тут не нужен, т.к. файлы закрываются после каждой операции
    }
}
