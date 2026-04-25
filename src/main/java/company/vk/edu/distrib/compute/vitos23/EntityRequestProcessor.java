package company.vk.edu.distrib.compute.vitos23;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.util.Map;

public interface EntityRequestProcessor extends AutoCloseable {
    void handleGet(
            HttpExchange exchange,
            String id,
            Map<String, String> queryParams
    ) throws IOException;

    void handlePut(
            HttpExchange exchange,
            String id,
            Map<String, String> queryParams
    ) throws IOException;

    void handleDelete(
            HttpExchange exchange,
            String id,
            Map<String, String> queryParams
    ) throws IOException;

    @Override
    default void close() throws Exception {
        // default
    }
}
