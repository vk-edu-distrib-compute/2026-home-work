package company.vk.edu.distrib.compute.vitos23;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;

public interface EntityRequestProcessor {
    void handleGet(HttpExchange exchange, String id) throws IOException;

    void handlePut(HttpExchange exchange, String id) throws IOException;

    void handleDelete(HttpExchange exchange, String id) throws IOException;
}
