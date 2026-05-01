package company.vk.edu.distrib.compute.nesterukia.file_system;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.nesterukia.KVServiceImpl;
import company.vk.edu.distrib.compute.nesterukia.cluster.Router;

import java.io.IOException;

public class NesterukiaFileSystemKVServiceFactory extends KVServiceFactory {
    private static final String PERSISTENT_STORAGE_PATH = "storage";
    private final Router router;

    public NesterukiaFileSystemKVServiceFactory() {
        this(null);
    }

    public NesterukiaFileSystemKVServiceFactory(Router router) {
        super();
        this.router = router;
    }

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new KVServiceImpl(
                port,
                new NesterukiaFileSystemKVDao(PERSISTENT_STORAGE_PATH),
                router
        );
    }
}
