package company.vk.edu.distrib.compute.shuuuurik;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.shuuuurik.config.ReplicationConfig;
import company.vk.edu.distrib.compute.shuuuurik.routing.ReplicaRouter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Создаёт {@link KVServiceReplicaImpl} с кластером из {@value CLUSTER_SIZE} файловых узлов
 * и фактором репликации N из конфига.
 *
 * <p>Структура на диске:
 * <pre>
 *   /tmp/shuuuurik-replicas/node-0/
 *   /tmp/shuuuurik-replicas/node-1/
 *   ...
 *   /tmp/shuuuurik-replicas/node-9/
 * </pre>
 *
 * <p>Каждый ключ хранится на N из 10 узлов (выбор через Rendezvous Hashing).
 */
public class ShuuuurikReplicationKVServiceFactory extends KVServiceFactory {

    /**
     * Общее количество узлов в кластере.
     */
    static final int CLUSTER_SIZE = 10;

    private static final String REPLICA_BASE_DIR = "shuuuurik-replicas";

    @Override
    protected KVService doCreate(int port) throws IOException {
        ReplicationConfig config = new ReplicationConfig();
        int n = config.getReplicationFactor();

        Path baseDir = Path.of(System.getProperty("java.io.tmpdir"), REPLICA_BASE_DIR);
        List<Dao<byte[]>> allNodes = createNodes(CLUSTER_SIZE, baseDir);
        ReplicaRouter router = new ReplicaRouter(n);

        return new KVServiceReplicaImpl(port, allNodes, router);
    }

    /**
     * Создаёт все узлы кластера. Каждый узел - отдельный {@link FileDao}.
     *
     * @param count   количество узлов кластера
     * @param baseDir корневая директория для всех узлов
     * @return список из count {@link FileDao}
     * @throws IOException если не удалось создать директорию для какой-либо реплики
     */
    private List<Dao<byte[]>> createNodes(int count, Path baseDir) throws IOException {
        List<Dao<byte[]>> nodes = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Path replicaDir = baseDir.resolve("node-" + i);
            nodes.add(new FileDao(replicaDir));
        }
        return nodes;
    }
}
