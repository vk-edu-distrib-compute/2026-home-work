package company.vk.edu.distrib.compute.dariaprindina;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;
import java.nio.file.Path;

public class DPKvServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        final Path storageDir = Path.of("daria-prindina-storage", "port-" + port);
        return new DPKvService(port, new DPFileDao(storageDir));
    }
}
