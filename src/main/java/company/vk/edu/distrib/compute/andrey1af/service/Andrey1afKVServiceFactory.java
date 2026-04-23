package company.vk.edu.distrib.compute.andrey1af.service;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.andrey1af.dao.InFileDao;
import company.vk.edu.distrib.compute.andrey1af.sharding.HashRouter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

public class Andrey1afKVServiceFactory extends KVServiceFactory {
    private final Path dataRoot;

    public Andrey1afKVServiceFactory() {
        this(Path.of(".data"));
    }

    public Andrey1afKVServiceFactory(Path dataRoot) {
        this.dataRoot = Objects.requireNonNull(dataRoot, "dataRoot cannot be null");
    }

    private Path storagePath(int port) {
        return dataRoot.resolve("node-" + port);
    }

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new Andrey1afKVService(port, new InFileDao(storagePath(port)));
    }

    public Andrey1afKVService createClusterNode(
            int port, String selfEndpoint, HashRouter hashRouter) throws IOException {
        return new Andrey1afKVService(port, new InFileDao(storagePath(port)), selfEndpoint, hashRouter);
    }
}
