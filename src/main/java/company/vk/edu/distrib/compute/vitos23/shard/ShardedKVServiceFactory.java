package company.vk.edu.distrib.compute.vitos23.shard;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.vitos23.DirectEntityRequestProcessor;
import company.vk.edu.distrib.compute.vitos23.EntityRequestProcessor;
import company.vk.edu.distrib.compute.vitos23.KVServiceImpl;
import company.vk.edu.distrib.compute.vitos23.WalBackedDao;

import java.io.IOException;
import java.net.http.HttpClient;

import static company.vk.edu.distrib.compute.vitos23.util.HttpUtils.getLocalEndpoint;

public class ShardedKVServiceFactory extends KVServiceFactory {
    private final ShardResolver shardResolver;

    public ShardedKVServiceFactory(ShardResolver shardResolver) {
        super();
        this.shardResolver = shardResolver;
    }

    @Override
    protected KVService doCreate(int port) throws IOException {
        Dao<byte[]> dao = new WalBackedDao("storage/vitos23/shard-" + port);
        EntityRequestProcessor shardedEntityRequestProcessor = new ShardedEntityRequestProcessor(
                getLocalEndpoint(port),
                shardResolver,
                new DirectEntityRequestProcessor(dao),
                HttpClient.newHttpClient()
        );
        return new KVServiceImpl(port, shardedEntityRequestProcessor);
    }
}
