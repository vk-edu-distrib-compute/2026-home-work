package company.vk.edu.distrib.compute.solntseva_nastya;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;

public class SolntsevaKVServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        String myUrl = "http://localhost:" + port;
        return new SolntsevaKVService(
            port, 
            new PersistentDao(Paths.get("storage", "data_" + port)), 
            Collections.singleton(myUrl), 
            myUrl
        );
    }
}
