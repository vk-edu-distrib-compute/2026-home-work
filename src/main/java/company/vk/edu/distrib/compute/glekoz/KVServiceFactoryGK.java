package company.vk.edu.distrib.compute.glekoz;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

public class KVServiceFactoryGK extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        return new KVServiceGK(port);
    }
}
