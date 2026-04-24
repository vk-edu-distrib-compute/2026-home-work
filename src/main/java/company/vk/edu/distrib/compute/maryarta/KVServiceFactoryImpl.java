package company.vk.edu.distrib.compute.maryarta;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.maryarta.replication.ReplicatedServiceImpl;
import company.vk.edu.distrib.compute.maryarta.replication.ReplicationService;

import java.io.IOException;

public class KVServiceFactoryImpl extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        return new KVServiceImpl(port);
    }
}
