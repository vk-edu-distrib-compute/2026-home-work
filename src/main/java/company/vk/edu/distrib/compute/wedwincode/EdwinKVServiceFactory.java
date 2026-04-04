package company.vk.edu.distrib.compute.wedwincode;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;
import java.nio.file.Path;

public class EdwinKVServiceFactory extends KVServiceFactory {
    private static final Path DATA_DIR = Path.of(".data", "wedwincode");

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new KVServiceImpl(port, new PersistentDao(DATA_DIR));
    }
}
