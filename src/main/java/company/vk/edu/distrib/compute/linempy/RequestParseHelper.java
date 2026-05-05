package company.vk.edu.distrib.compute.linempy;

import com.sun.net.httpserver.HttpExchange;

public final class RequestParseHelper {
    private static final String ID_PARAM = "id";
    private static final String ACK_PARAM = "ack=";

    private RequestParseHelper() {
    }

    public static String extractId(HttpExchange exchange) {
        String query = exchange.getRequestURI().getQuery();
        if (query == null) {
            return null;
        }
        for (String param : query.split("&")) {
            String[] pair = param.split("=");
            if (pair.length == 2 && ID_PARAM.equals(pair[0])) {
                return pair[1];
            }
        }
        return null;
    }

    public static int extractAck(HttpExchange exchange, int defaultValue) {
        String query = exchange.getRequestURI().getQuery();
        if (query == null) {
            return defaultValue;
        }
        for (String param : query.split("&")) {
            if (param.startsWith(ACK_PARAM)) {
                try {
                    return Integer.parseInt(param.substring(4));
                } catch (NumberFormatException e) {
                    return defaultValue;
                }
            }
        }
        return defaultValue;
    }
}
