package company.vk.edu.distrib.compute.usl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PersistentByteArrayDaoTest {
    private static final String ALPHA_KEY = "alpha";
    private static final String SPECIAL_KEY = "space key/with?chars";

    @TempDir
    Path tempDir;

    @Test
    void persistsAcrossReopen() throws IOException {
        Path storage = tempDir.resolve("storage");
        byte[] value = new byte[] {1, 2, 3};

        try (PersistentByteArrayDao firstDao = new PersistentByteArrayDao(storage)) {
            firstDao.upsert(ALPHA_KEY, value);
        }

        try (PersistentByteArrayDao secondDao = new PersistentByteArrayDao(storage)) {
            assertArrayEquals(value, secondDao.get(ALPHA_KEY));
        }
    }

    @Test
    void deletePersistsAcrossReopen() throws IOException {
        Path storage = tempDir.resolve("storage");

        try (PersistentByteArrayDao firstDao = new PersistentByteArrayDao(storage)) {
            firstDao.upsert(ALPHA_KEY, new byte[] {1});
            firstDao.delete(ALPHA_KEY);
        }

        try (PersistentByteArrayDao secondDao = new PersistentByteArrayDao(storage)) {
            assertThrows(NoSuchElementException.class, () -> secondDao.get(ALPHA_KEY));
        }
    }

    @Test
    void persistsEmptyValue() throws IOException {
        Path storage = tempDir.resolve("storage");
        byte[] value = new byte[0];

        try (PersistentByteArrayDao firstDao = new PersistentByteArrayDao(storage)) {
            firstDao.upsert(SPECIAL_KEY, value);
        }

        try (PersistentByteArrayDao secondDao = new PersistentByteArrayDao(storage)) {
            assertArrayEquals(value, secondDao.get(SPECIAL_KEY));
        }
    }
}
