package company.vk.edu.distrib.compute.gavrilova_ekaterina;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

public final class RequestParserHelper {

    private RequestParserHelper() {
    }

    public static String extractId(HttpExchange exchange) {
        String query = exchange.getRequestURI().getQuery();
        if (query == null || !query.contains("id=")) {
            return null;
        }
        for (String param : query.split("&")) {
            if (param.startsWith("id=")) {
                return URLDecoder.decode(param.substring(3), StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    public static int extractAck(HttpExchange exchange, int replicationFactor) throws IOException {
        String query = exchange.getRequestURI().getQuery();
        if (query == null) {
            return replicationFactor / 2 + 1;
        }
        for (String param : query.split("&")) {
            if (param.startsWith("ack=")) {
                String value = param.substring(4);
                try {
                    return Integer.parseInt(value);
                } catch (NumberFormatException e) {
                    return replicationFactor / 2 + 1;
                }
            }
        }
        return replicationFactor / 2 + 1;
    }

}
