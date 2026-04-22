package company.vk.edu.distrib.compute.marinchanka;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import java.io.IOException;

public class MarinchankaKVServiceFactory extends KVServiceFactory {

    @Override
    protected KVService doCreate(int port) throws IOException {
        PersistentDao dao = new PersistentDao("./data");
        return new MarinchankaKVService(port, dao);
    }
}
