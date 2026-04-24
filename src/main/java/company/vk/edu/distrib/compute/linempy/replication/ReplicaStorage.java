package company.vk.edu.distrib.compute.linempy.replication;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Файловое хранилище реплик.
 *
 * @author Linempy
 * @since 24.04.2026
 */
public class ReplicaStorage {
    private final Path dir;

    public ReplicaStorage(ReplicationConfig config, int port) throws IOException {
        this.dir = Paths.get(config.getStoragePath(), "node" + port, "replicas");
        Files.createDirectories(dir);
    }

    public Path getPath(String key, int idx) {
        return dir.resolve(key + "_r" + idx + ".dat");
    }

    public void write(String key, int idx, byte[] value) throws IOException {
        Files.write(getPath(key, idx), value);
    }

    public byte[] read(String key, int idx) throws IOException {
        return Files.readAllBytes(getPath(key, idx));
    }

    public boolean exists(String key, int idx) {
        return Files.exists(getPath(key, idx));
    }

    public boolean delete(String key, int idx) throws IOException {
        return Files.deleteIfExists(getPath(key, idx));
    }

    public long getKeyCountForReplica(int replicaId) throws IOException {
        try (var stream = Files.list(dir)) {
            return stream.filter(path -> path.toString().endsWith("_r" + replicaId + ".dat"))
                    .count();
        }
    }
}
