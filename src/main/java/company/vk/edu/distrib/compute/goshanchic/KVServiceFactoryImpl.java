package company.vk.edu.distrib.compute.goshanchic;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;
import java.util.List;

public class KVServiceFactoryImpl extends KVServiceFactory {
    private static final int DEFAULT_REPLICATION_FACTOR = 1;
    private static final int DEFAULT_ACK = 1;

    private final int replicationFactor;
    private final int defaultAck;

    public KVServiceFactoryImpl() {
        this(DEFAULT_REPLICATION_FACTOR, DEFAULT_ACK);
    }

    public KVServiceFactoryImpl(int replicationFactor, int defaultAck) {
        this.replicationFactor = replicationFactor;
        this.defaultAck = defaultAck;
    }

    @Override
    protected KVService doCreate(int port) throws IOException {
        // одиночный режим (без кластера)
        return doCreate(port, List.of(port));
    }

    @Override
    protected KVService doCreate(int port, List<Integer> clusterPorts) throws IOException {
        InMemoryDao dao = new InMemoryDao();

        // Передаём ВЕСЬ кластер, а не одну ноду
        return new KVServiceImpl(
                port,
                clusterPorts,
                dao,
                replicationFactor,
                defaultAck
        );
    }
}
