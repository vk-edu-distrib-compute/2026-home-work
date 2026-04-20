package company.vk.edu.distrib.compute.andrey1af.service;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.andrey1af.dao.InFileDao;
import company.vk.edu.distrib.compute.andrey1af.sharding.HashRouter;

import java.io.IOException;
import java.nio.file.Path;

public class Andrey1afKVServiceFactory extends KVServiceFactory {
    private static final Path DATA_ROOT = Path.of(".data");

    private static Path storagePath(int port) {
        return DATA_ROOT.resolve(Integer.toString(port));
    }

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new Andrey1afKVService(port, new InFileDao(storagePath(port)));
    }

    public Andrey1afKVService createClusterNode(int port, String selfEndpoint,
                                                HashRouter hashRouter) throws IOException {
        return new Andrey1afKVService(port, new InFileDao(storagePath(port)), selfEndpoint, hashRouter);
    }
}
