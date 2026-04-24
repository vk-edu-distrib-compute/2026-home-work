package company.vk.edu.distrib.compute.martinez1337.util;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class QueryHelper {
    private QueryHelper() {
    }

    public static Map<String, List<String>> parseParams(String rawQuery) {
        Map<String, List<String>> queryPairs = new ConcurrentHashMap<>();

        if (rawQuery == null || rawQuery.isBlank()) {
            return queryPairs;
        }

        String[] pairs = rawQuery.split("&");

        for (String pair : pairs) {
            int idx = pair.indexOf('=');
            String key;
            String value;

            if (idx > 0) {
                key = URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8);
                value = URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8);
            } else if (idx == 0) {
                continue;
            } else {
                key = URLDecoder.decode(pair, StandardCharsets.UTF_8);
                value = "";
            }

            addValueToMap(queryPairs, key, value);
        }
        return queryPairs;
    }

    private static void addValueToMap(Map<String, List<String>> map, String key, String value) {
        List<String> values = map.computeIfAbsent(key, k -> new ArrayList<>());
        values.add(value);
    }
}
