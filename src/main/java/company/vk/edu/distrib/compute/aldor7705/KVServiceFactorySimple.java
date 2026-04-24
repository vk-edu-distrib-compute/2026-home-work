package company.vk.edu.distrib.compute.aldor7705;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class KVServiceFactorySimple extends KVServiceFactory {
    private final String baseStorageDir;
    private List<Integer> clusterPorts;
    private final int replicas;

    public KVServiceFactorySimple() {
        this("storage", null, 3);
    }

    public KVServiceFactorySimple(String baseStorageDir, List<Integer> clusterPorts, int replicas) {
        super();
        this.baseStorageDir = baseStorageDir;
        this.clusterPorts = clusterPorts;
        this.replicas = replicas;
    }

    public KVServiceFactorySimple(String baseStorageDir, List<Integer> clusterPorts) {
        this(baseStorageDir, clusterPorts, 3);
    }

    @Override
    protected KVService doCreate(int port) throws IOException {
        Path pathOfStorage = Path.of(baseStorageDir + "_" + port);

        if (!Files.exists(pathOfStorage)) {
            Files.createDirectory(pathOfStorage);
        }
//        Path filePath = pathOfStorage.resolve("storage_" + port + ".txt");
        return new KVServiceSimple(port, new EntityDao(pathOfStorage, replicas), clusterPorts, replicas);
    }
}
