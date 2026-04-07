package company.vk.edu.distrib.compute.bushuev_a_s;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class MyKVServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        final Path tempPath = Files.createTempDirectory("my_kv_storage");
        return new MyKVService(port, new MyFileDao(tempPath));
    }
}
