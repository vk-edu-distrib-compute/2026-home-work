package company.vk.edu.distrib.compute.tadzhnahal;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class TadzhnahalKVServiceFactory extends KVServiceFactory {
    private static final String LOCALHOST = "http://localhost:";

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new TadzhnahalKVService(
                port,
                createDao(port),
                List.of(endpoint(port))
        );
    }

    public KVService createClusterNode(int port, List<String> clusterEndpoints) throws IOException {
        return new TadzhnahalKVService(
                port,
                createDao(port),
                clusterEndpoints
        );
    }

    private Dao<byte[]> createDao(int port) throws IOException {
        Path rootDir = Path.of(
                System.getProperty("user.home"),
                ".vk-distributed-computing",
                "tadzhnahal",
                "port-" + port
        );

        return new FileDao(rootDir);
    }

    private String endpoint(int port) {
        return LOCALHOST + port;
    }
}
