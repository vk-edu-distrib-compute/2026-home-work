package company.vk.edu.distrib.compute.mandesero;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;
import java.nio.file.Files;

public class KVServiceFactoryImpl extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        // Isolated directory per instance; shared default `.data` caused flaky integration tests on CI.
        return new KVServiceImpl(port, new FSDao(Files.createTempDirectory("mandesero-kv-")));
    }
}
