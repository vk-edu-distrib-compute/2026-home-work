package company.vk.edu.distrib.compute.mediocritas.service;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;

public class StandaloneKvByteService extends AbstractKvByteService {

    public StandaloneKvByteService(int port, Dao<byte[]> dao) {
        super(port, dao);
    }

    @Override
    protected void handleEntity(HttpExchange http) throws IOException {
        String id = parseId(http.getRequestURI().getQuery());
        handleEntityLocally(http, id);
    }
}
