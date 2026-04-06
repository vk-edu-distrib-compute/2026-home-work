package company.vk.edu.distrib.compute.bahadir_ahmedov;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

public class BahadirAhmedovKVServiceFactory extends KVServiceFactory {

    @Override
    protected KVService doCreate(int port) throws IOException {

        return new BahadirAhmedovKVService(port);
    }
}
