package company.vk.edu.distrib.compute.tadzhnahal;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;
import java.nio.file.Path;

public class TadzhnahalKVServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        Path rootDir = Path.of(
                System.getProperty("user.home"),
                ".vk-distributed-computing",
                "tadzhnahal",
                "port-" + port
        );

        FileDao dao = new FileDao(rootDir);
        return new TadzhnahalKVService(port, dao);
    }
}
