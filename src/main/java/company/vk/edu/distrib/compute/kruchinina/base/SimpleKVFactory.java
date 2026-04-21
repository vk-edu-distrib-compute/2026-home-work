package company.vk.edu.distrib.compute.kruchinina.base;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Фабрика для KV сервиса.
 */
public class SimpleKVFactory extends KVServiceFactory {
    private static final String DEFAULT_STORAGE_DIR = "./data";

    @Override
    protected KVService doCreate(final int port) throws IOException {
        final Path storagePath = Path.of(DEFAULT_STORAGE_DIR);
        if (!Files.exists(storagePath)) {
            Files.createDirectories(storagePath);
        }
        final Dao<byte[]> dao = new FileSystemDao(DEFAULT_STORAGE_DIR);
        return new SimpleKVService(port, dao);
    }
}
