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
        super();
        this.replicationFactor = DEFAULT_REPLICATION_FACTOR;
        this.defaultAck = DEFAULT_ACK;
    }

    public KVServiceFactoryImpl(int replicationFactor, int defaultAck) {
        super();
        this.replicationFactor = replicationFactor;
        this.defaultAck = defaultAck;
    }

    @Override
    protected KVService doCreate(int port) throws IOException {
        InMemoryDao dao = new InMemoryDao();
        return new KVServiceImpl(port, List.of(port), dao, replicationFactor, defaultAck);
    }
}
