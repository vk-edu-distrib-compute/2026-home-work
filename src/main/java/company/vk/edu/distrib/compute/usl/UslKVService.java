package company.vk.edu.distrib.compute.usl;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;

public class UslKVService implements KVService {
    private final UslNodeServer node;

    public UslKVService(int port) throws IOException {
        this(port, new PersistentByteArrayDao(StoragePaths.persistentDataDir(port)));
    }

    UslKVService(int port, Dao<byte[]> dao) {
        this.node = new UslNodeServer(port, dao);
    }

    @Override
    public void start() {
        node.start();
    }

    @Override
    public void stop() {
        node.stop();
    }
}
