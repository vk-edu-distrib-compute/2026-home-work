package company.vk.edu.distrib.compute.usl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PersistentByteArrayDaoTest {
    @TempDir
    Path tempDir;

    @Test
    void persistsAcrossReopen() throws IOException {
        Path storage = tempDir.resolve("storage");
        byte[] value = new byte[] {1, 2, 3};

        PersistentByteArrayDao firstDao = new PersistentByteArrayDao(storage);
        firstDao.upsert("alpha", value);
        firstDao.close();

        PersistentByteArrayDao secondDao = new PersistentByteArrayDao(storage);
        assertArrayEquals(value, secondDao.get("alpha"));
    }

    @Test
    void deletePersistsAcrossReopen() throws IOException {
        Path storage = tempDir.resolve("storage");

        PersistentByteArrayDao firstDao = new PersistentByteArrayDao(storage);
        firstDao.upsert("alpha", new byte[] {1});
        firstDao.delete("alpha");
        firstDao.close();

        PersistentByteArrayDao secondDao = new PersistentByteArrayDao(storage);
        assertThrows(NoSuchElementException.class, () -> secondDao.get("alpha"));
    }

    @Test
    void persistsEmptyValue() throws IOException {
        Path storage = tempDir.resolve("storage");
        byte[] value = new byte[0];

        PersistentByteArrayDao firstDao = new PersistentByteArrayDao(storage);
        firstDao.upsert("space key/with?chars", value);
        firstDao.close();

        PersistentByteArrayDao secondDao = new PersistentByteArrayDao(storage);
        assertArrayEquals(value, secondDao.get("space key/with?chars"));
    }
}
