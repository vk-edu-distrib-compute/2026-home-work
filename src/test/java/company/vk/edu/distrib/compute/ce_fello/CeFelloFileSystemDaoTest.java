package company.vk.edu.distrib.compute.ce_fello;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CeFelloFileSystemDaoTest {

    @TempDir
    Path tempDirectory;

    @Test
    void persistsValueAfterReopen() throws IOException {
        byte[] value = new byte[]{1, 2, 3};

        CeFelloFileSystemDao firstDao = new CeFelloFileSystemDao(tempDirectory);
        firstDao.upsert("key", value);
        firstDao.close();

        CeFelloFileSystemDao secondDao = new CeFelloFileSystemDao(tempDirectory);
        assertArrayEquals(value, secondDao.get("key"));
    }

    @Test
    void missingKeyThrows() throws IOException {
        CeFelloFileSystemDao dao = new CeFelloFileSystemDao(tempDirectory);

        assertThrows(NoSuchElementException.class, () -> dao.get("absent"));
    }

    @Test
    void upsertOverwritesValue() throws IOException {
        CeFelloFileSystemDao dao = new CeFelloFileSystemDao(tempDirectory);

        dao.upsert("key", new byte[]{1});
        dao.upsert("key", new byte[]{2, 3});

        assertArrayEquals(new byte[]{2, 3}, dao.get("key"));
    }

    @Test
    void deleteRemovesValue() throws IOException {
        CeFelloFileSystemDao dao = new CeFelloFileSystemDao(tempDirectory);

        dao.upsert("key", new byte[]{1});
        dao.delete("key");

        assertThrows(NoSuchElementException.class, () -> dao.get("key"));
    }

    @Test
    void storesEmptyValue() throws IOException {
        CeFelloFileSystemDao dao = new CeFelloFileSystemDao(tempDirectory);

        dao.upsert("key", new byte[0]);

        assertArrayEquals(new byte[0], dao.get("key"));
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
