package company.vk.edu.distrib.compute.ce_fello;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CeFelloFileSystemDaoTest {
    private static final String TEST_KEY = "key";

    @TempDir
    Path tempDirectory;

    @Test
    void persistsValueAfterReopen() throws IOException {
        byte[] value = new byte[]{1, 2, 3};

        CeFelloFileSystemDao firstDao = new CeFelloFileSystemDao(tempDirectory);
        firstDao.upsert(TEST_KEY, value);
        firstDao.close();

        CeFelloFileSystemDao secondDao = new CeFelloFileSystemDao(tempDirectory);
        assertArrayEquals(value, secondDao.get(TEST_KEY));
    }

    @Test
    void missingKeyThrows() throws IOException {
        CeFelloFileSystemDao dao = new CeFelloFileSystemDao(tempDirectory);

        assertThrows(NoSuchElementException.class, () -> dao.get("absent"));
    }

    @Test
    void upsertOverwritesValue() throws IOException {
        CeFelloFileSystemDao dao = new CeFelloFileSystemDao(tempDirectory);

        dao.upsert(TEST_KEY, new byte[]{1});
        dao.upsert(TEST_KEY, new byte[]{2, 3});

        assertArrayEquals(new byte[]{2, 3}, dao.get(TEST_KEY));
    }

    @Test
    void deleteRemovesValue() throws IOException {
        CeFelloFileSystemDao dao = new CeFelloFileSystemDao(tempDirectory);

        dao.upsert(TEST_KEY, new byte[]{1});
        dao.delete(TEST_KEY);

        assertThrows(NoSuchElementException.class, () -> dao.get(TEST_KEY));
    }

    @Test
    void storesEmptyValue() throws IOException {
        CeFelloFileSystemDao dao = new CeFelloFileSystemDao(tempDirectory);

        dao.upsert(TEST_KEY, new byte[0]);

        assertArrayEquals(new byte[0], dao.get(TEST_KEY));
    }

    @Test
    void storesUnsafeKeyAsData() throws IOException {
        CeFelloFileSystemDao dao = new CeFelloFileSystemDao(tempDirectory);
        String key = "../key with spaces?x=1";
        byte[] value = new byte[]{4, 5, 6};

        dao.upsert(key, value);

        assertArrayEquals(value, dao.get(key));
    }
}
