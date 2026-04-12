package company.vk.edu.distrib.compute.golubtsov_pavel;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;
import java.nio.file.Path;

public class PGInMemoryKVServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        return new PGInMemoryKVService(port, new PGFileDao(Path.of("PGData")));
    }
}
