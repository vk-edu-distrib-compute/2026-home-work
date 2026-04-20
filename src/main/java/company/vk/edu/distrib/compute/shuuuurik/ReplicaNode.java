package company.vk.edu.distrib.compute.shuuuurik;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Файловый узел кластера с поддержкой версионированных записей и управляемого отключения.
 *
 * <p>Хранит {@link VersionedEntry} на диске: каждый ключ = один файл в {@code rootDir}.
 * Поддерживает флаг {@code enabled} - при отключении все операции бросают {@link IOException},
 * что позволяет симулировать недоступность узла в тестах.
 */
public class ReplicaNode {

    private static final Logger log = LoggerFactory.getLogger(ReplicaNode.class);

    private final int nodeId;
    private final Path rootDir;

    private final AtomicBoolean enabled = new AtomicBoolean(true);

    /**
     * Создаёт узел кластера.
     *
     * @param nodeId  порядковый номер узла
     * @param rootDir директория для хранения файлов этого узла
     * @throws IOException если не удалось создать директорию
     */
    ReplicaNode(int nodeId, Path rootDir) throws IOException {
        this.nodeId = nodeId;
        this.rootDir = rootDir;
        Files.createDirectories(rootDir);
    }

    private static String encodeKey(String key) {
        return Base64.getUrlEncoder().encodeToString(key.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Выключает узел. Все последующие операции будут бросать {@link IOException}.
     */
    void disable() {
        enabled.set(false);
        log.debug("Node-{} disabled", nodeId);
    }

    /**
     * Включает узел. Операции снова доступны.
     */
    void enable() {
        enabled.set(true);
        log.debug("Node-{} enabled", nodeId);
    }

    /**
     * Читает версионированную запись по ключу.
     *
     * @param key ключ
     * @return Optional с записью, пустой если ключ не найден
     * @throws IOException если узел недоступен или ошибка I/O
     */
    Optional<VersionedEntry> read(String key) throws IOException {
        checkEnabled();
        Path path = keyPath(key);
        if (!Files.exists(path)) {
            return Optional.empty();
        }
        try (ObjectInputStream ois = new ObjectInputStream(
                new BufferedInputStream(Files.newInputStream(path)))) {
            return Optional.of((VersionedEntry) ois.readObject());
        } catch (ClassNotFoundException e) {
            throw new IOException("Corrupted data for key: " + key, e);
        }
    }

    /**
     * Записывает версионированную запись по ключу (атомарно через tmp-файл).
     *
     * @param key   ключ
     * @param entry запись для сохранения
     * @throws IOException если узел недоступен или ошибка I/O
     */
    void write(String key, VersionedEntry entry) throws IOException {
        checkEnabled();
        Path target = keyPath(key);
        Path temp = rootDir.resolve(encodeKey(key) + ".tmp");

        try (ObjectOutputStream oos = new ObjectOutputStream(
                new BufferedOutputStream(Files.newOutputStream(temp)))) {
            oos.writeObject(entry);
        }

        Files.move(temp, target,
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE);
    }

    /**
     * Проверяет доступность узла.
     *
     * @throws IOException если узел отключён
     */
    private void checkEnabled() throws IOException {
        if (!enabled.get()) {
            throw new IOException("Node-" + nodeId + " is disabled");
        }
    }

    private Path keyPath(String key) {
        return rootDir.resolve(encodeKey(key));
    }
}
