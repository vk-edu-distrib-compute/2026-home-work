package company.vk.edu.distrib.compute.ip;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import java.io.IOException;
import java.util.function.Function;

public class PopovIgorKVServiceFactoryImpl extends KVServiceFactory {
    private Function<String, String> router;

    public void setRouter(Function<String, String> router) {
        this.router = router;
    }

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new PopovIgorKVService(port, router);
    }
}
