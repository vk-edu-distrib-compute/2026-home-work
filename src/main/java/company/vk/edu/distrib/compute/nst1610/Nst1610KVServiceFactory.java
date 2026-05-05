package company.vk.edu.distrib.compute.nst1610;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import java.io.IOException;

public class Nst1610KVServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        return new Nst1610KVService(port);
    }
}
