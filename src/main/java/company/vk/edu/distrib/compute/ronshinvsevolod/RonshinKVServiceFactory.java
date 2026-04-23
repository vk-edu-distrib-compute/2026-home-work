package company.vk.edu.distrib.compute.ronshinvsevolod;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.NoSuchElementException;

public class RonshinKVServiceFactory extends KVServiceFactory {
    
    @Override
    protected KVService doCreate(int port) {
        List<Dao<byte[]>> replicas = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            replicas.add(new LocalFileDao(port, i));
        }
        return new ReplicatedKVService(port, replicas);
    }

    private static final class LocalFileDao implements Dao<byte[]> {
        private final Path dir;
        private final ReadWriteLock lock = new ReentrantReadWriteLock();

        LocalFileDao(int port, int replicaId) {
            this.dir = Paths.get(".", "data", "node_" + port + "_replica_" + replicaId);
            try {
                Files.createDirectories(dir);
            } catch (IOException e) {
                throw new IllegalStateException("Cannot create dir", e);
            }
        }

        private Path getPath(String key) {
            String safeName = Base64.getUrlEncoder().encodeToString(key.getBytes(StandardCharsets.UTF_8));
            return dir.resolve(safeName);
        }

        @Override
        public byte[] get(String key) throws NoSuchElementException {
            Path path = getPath(key);
            lock.readLock().lock();
            try {
                if (!Files.exists(path)) {
                    throw new NoSuchElementException("File does not exist");
                }
                return Files.readAllBytes(path);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            } finally {
                lock.readLock().unlock();
            }
        }

        @Override
        public void upsert(String key, byte[] value) {
            Path path = getPath(key);
            lock.writeLock().lock();
            try {
                Files.write(path, value, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            } finally {
                lock.writeLock().unlock();
            }
        }

        @Override
        public void delete(String key) {
            Path path = getPath(key);
            lock.writeLock().lock();
            try {
                Files.deleteIfExists(path);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            } finally {
                lock.writeLock().unlock();
            }
        }

        @Override
        public void close() {
            // .
        }
    }
}
