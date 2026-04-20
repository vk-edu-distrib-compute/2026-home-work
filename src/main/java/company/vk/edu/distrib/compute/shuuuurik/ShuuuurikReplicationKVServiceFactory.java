package company.vk.edu.distrib.compute.shuuuurik;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.shuuuurik.config.ReplicationConfig;
import company.vk.edu.distrib.compute.shuuuurik.routing.ReplicaRouter;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Создаёт {@link KVServiceReplicaImpl} с файловыми узлами и фактором репликации N из конфига.
 *
 * <p>Поддерживает два режима работы (переключается флагом {@link #FULL_REPLICATION}):
 * <ul>
 *   <li>{@code true} - количество узлов = N (каждый ключ на всех узлах,
 *       disableReplica гарантированно влияет на любой ключ)</li>
 *   <li>{@code false} - количество узлов = {@value CLUSTER_SIZE} (каждый ключ
 *       на N из {@value CLUSTER_SIZE} узлов, выбор через Rendezvous Hashing)</li>
 * </ul>
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
     * Режим полной репликации: количество узлов = фактор репликации N.
     * Каждый ключ хранится на всех узлах.
     *
     * <p>При {@code false} создаётся кластер из {@value CLUSTER_SIZE} узлов,
     * и каждый ключ хранится на N из них (частичная репликация).
     */
    private static final boolean FULL_REPLICATION = true;

    /**
     * Размер кластера при частичной репликации ({@link #FULL_REPLICATION} = false).
     * Игнорируется в режиме полной репликации.
     */
    static final int CLUSTER_SIZE = ReplicationConfig.MAX_CLUSTER_SIZE;

    private static final String REPLICA_BASE_DIR = "shuuuurik-replicas";

    @Override
    protected KVService doCreate(int port) throws IOException {
        ReplicationConfig config = new ReplicationConfig();
        int n = config.getReplicationFactor();

        int nodeCount = FULL_REPLICATION ? n : CLUSTER_SIZE;

        Path baseDir = Path.of(System.getProperty("java.io.tmpdir"), REPLICA_BASE_DIR);
        ReplicaNode[] nodes = createNodes(nodeCount, baseDir);
        ReplicaRouter router = new ReplicaRouter(n);

        return new KVServiceReplicaImpl(port, nodes, router);
    }

    /**
     * Создаёт все узлы кластера. Каждый узел - отдельный {@link ReplicaNode}.
     *
     * @param count   количество узлов кластера
     * @param baseDir корневая директория для всех узлов
     * @return массив из count {@link ReplicaNode}
     * @throws IOException если не удалось создать директорию для какой-либо реплики
     */
    private ReplicaNode[] createNodes(int count, Path baseDir) throws IOException {
        ReplicaNode[] nodes = new ReplicaNode[count];
        for (int i = 0; i < count; i++) {
            Path replicaDir = baseDir.resolve("node-" + i);
            nodes[i] = new ReplicaNode(i, replicaDir);
        }
        return nodes;
    }
}
