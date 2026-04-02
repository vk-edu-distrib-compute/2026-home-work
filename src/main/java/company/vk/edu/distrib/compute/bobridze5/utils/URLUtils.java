package company.vk.edu.distrib.compute.bobridze5.utils;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class URLUtils {
    private static final String PARAM_SPLITTER = "&";
    private static final String PARAM_EQUALS = "=";

    private URLUtils() {
    }

    public static Map<String, String> parseQueryParams(String rawQuery) {
        if (rawQuery == null || rawQuery.isBlank()) {
            return Collections.emptyMap();
        }

        Map<String, String> params = new ConcurrentHashMap<>();

        for (String pair : rawQuery.split(PARAM_SPLITTER)) {
            String[] parts = pair.split(PARAM_EQUALS, 2);

            String key = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
            String value = parts.length > 1
                    ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8)
                    : "";

            params.put(key, value);
        }

        return params;
    }
}
