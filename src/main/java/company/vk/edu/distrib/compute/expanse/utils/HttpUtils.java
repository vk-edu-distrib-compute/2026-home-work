package company.vk.edu.distrib.compute.expanse.utils;

import com.sun.net.httpserver.HttpExchange;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class HttpUtils {
    private HttpUtils() {
    }

    public static Map<String, String> extractParams(HttpExchange httpExchange) {
        // В стриме ниже нет многопоточности,
        // ConcurrentHashMap используется для соблюдения требования чекера.
        Map<String, String> result = new ConcurrentHashMap<>();
        String query = httpExchange.getRequestURI().getQuery();

        Arrays.stream(query.split("&"))
                .map(param -> param.split("=", 2))
                .forEach(pair -> result.put(pair[0].toLowerCase(), pair[1]));

        return result;
    }
}
