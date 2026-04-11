package company.vk.edu.distrib.compute.polozhentsev_ivan;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;
import java.nio.file.Path;

public final class PolozhentsevIvanKVServiceFactory extends KVServiceFactory {

    private static final String STORAGE_DIR = "polozhentsev_ivan_storage";

    @Override
    protected KVService doCreate(int port) throws IOException {
        Path dataDir = Path.of(STORAGE_DIR, "p" + port);
        return new PolozhentsevIvanKVService(port, new FileSystemDao(dataDir));
    }
}
