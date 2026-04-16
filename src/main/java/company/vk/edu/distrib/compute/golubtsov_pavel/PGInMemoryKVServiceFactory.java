package company.vk.edu.distrib.compute.golubtsov_pavel;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class PGInMemoryKVServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        String selfEndpoint = "http://localhost:" + port;
        List<String> endpoints = List.of(selfEndpoint);
        return new PGInMemoryKVService(port,
                new PGFileDao(Path.of("PGData", String.valueOf(port))),
                selfEndpoint,
                endpoints);
    }
}
