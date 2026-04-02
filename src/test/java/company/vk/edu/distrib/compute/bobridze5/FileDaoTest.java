package company.vk.edu.distrib.compute.bobridze5;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class FileDaoTest {
    @TempDir
    Path tempDir;

    private FileDao dao;

    @BeforeEach
    void setUp() throws IOException {
        dao = new FileDao(tempDir);
    }

    @Test
    void testUpsertAndGet() throws IOException {
        String key = "testKey";
        byte[] value = "hello world".getBytes(StandardCharsets.UTF_8);

        dao.upsert(key, value);
        byte[] result = dao.get(key);

        assertArrayEquals(value, result, "Данные должны совпадать после сохранения");
    }

    @Test
    void testGetNonExistentKey() {
        assertThrows(NoSuchElementException.class, () -> dao.get("missing"),
                "Должно бросать NoSuchElementException, если ключа нет");
    }

    @Test
    void testUpdateExistingKey() throws IOException {
        String key = "key1";
        dao.upsert(key, "old".getBytes());
        dao.upsert(key, "new".getBytes());

        assertArrayEquals("new".getBytes(), dao.get(key), "Данные должны обновиться");
    }

    @Test
    void testDelete() throws IOException {
        String key = "toDelete";
        dao.upsert(key, "data".getBytes());

        dao.delete(key);

        assertThrows(NoSuchElementException.class, () -> dao.get(key),
                "После удаления ключ не должен находиться");
    }

    @Test
    void testDeleteNonExistentKey() {
        assertDoesNotThrow(() -> dao.delete("unknown"),
                "Удаление несуществующего ключа не должно вызывать ошибку");
    }

    @Test
    void testInvalidKey() {
        assertThrows(IllegalArgumentException.class, () -> dao.upsert("", "data".getBytes()),
                "Пустой ключ должен вызывать IllegalArgumentException");
    }
}
