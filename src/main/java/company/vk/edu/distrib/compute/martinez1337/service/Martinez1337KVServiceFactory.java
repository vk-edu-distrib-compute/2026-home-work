package company.vk.edu.distrib.compute.martinez1337.service;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.martinez1337.dao.H2Dao;

import java.io.IOException;

public class Martinez1337KVServiceFactory extends KVServiceFactory {

    @Override
    protected KVService doCreate(int port) throws IOException {
        String dbUrl = "jdbc:h2:mem:kvstore_" + port + ";MODE=MySQL;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE";
        return new Martinez1337KVService(port, new H2Dao(dbUrl));
    }
}
