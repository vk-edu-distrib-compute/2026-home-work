package company.vk.edu.distrib.compute.ce_fello;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;
import java.nio.file.Path;

public class CeFelloKVServiceFactory extends KVServiceFactory {
    private static final Path STORAGE_ROOT = Path.of(".data", "ce_fello");

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new CeFelloKVService(port, new CeFelloFileSystemDao(STORAGE_ROOT.resolve(String.valueOf(port))));
    }
}
