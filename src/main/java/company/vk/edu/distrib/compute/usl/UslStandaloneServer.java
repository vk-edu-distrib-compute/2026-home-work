package company.vk.edu.distrib.compute.usl;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;

public final class UslStandaloneServer {
    private UslStandaloneServer() {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 8080;
        Path dataDirectory = args.length > 1
            ? Path.of(args[1])
            : StoragePaths.persistentDataDir(port);

        Dao<byte[]> dao = new PersistentByteArrayDao(dataDirectory);
        KVService service = new UslKVService(port, dao);
        service.start();
        Runtime.getRuntime().addShutdownHook(new Thread(service::stop));
        new CountDownLatch(1).await();
    }
}
