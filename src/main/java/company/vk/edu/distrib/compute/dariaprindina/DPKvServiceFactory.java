package company.vk.edu.distrib.compute.dariaprindina;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

public class DPKvServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        return new DPKvService(port, new DPDao());
    }
}
