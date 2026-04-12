package company.vk.edu.distrib.compute.lillymega;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;
import java.nio.file.Path;

public class LillymegaKVServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        Path dataFile = Path.of("tmp", "dao-" + port + ".data");
        return new LillymegaKVService(port, new PersistentDao(dataFile));
        //return new LillymegaKVService(port, new LillymegaDao());  //main task
    }
}
