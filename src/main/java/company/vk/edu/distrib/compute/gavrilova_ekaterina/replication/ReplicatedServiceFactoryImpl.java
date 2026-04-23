package company.vk.edu.distrib.compute.gavrilova_ekaterina.replication;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

public class ReplicatedServiceFactoryImpl extends KVServiceFactory {

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new ReplicatedKVServiceImpl(port);
    }
}
