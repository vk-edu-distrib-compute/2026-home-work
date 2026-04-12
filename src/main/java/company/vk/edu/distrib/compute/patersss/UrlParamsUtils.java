package company.vk.edu.distrib.compute.patersss;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

public final class UrlParamsUtils {
    private static final String ID_PARAM = "id=";

    private UrlParamsUtils() {
    }

    public static String getIdFromQuery(String query) {
        if (query == null || query.isBlank()) {
            throw new IllegalArgumentException("Query is empty");
        }

        String[] params = query.split("&");
        for (String param : params) {
            if (param.startsWith(ID_PARAM)) {
                String id = URLDecoder.decode(
                        param.substring(ID_PARAM.length()),
                        StandardCharsets.UTF_8
                );

                if (id.isBlank()) {
                    throw new IllegalArgumentException("Id is empty");
                }

                return id;
            }
        }

        throw new IllegalArgumentException("Bad query. There is no id param");
    }
}
