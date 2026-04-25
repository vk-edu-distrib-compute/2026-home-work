package company.vk.edu.distrib.compute.vitos23.util;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class HttpUtils {
    public static final int NO_BODY_RESPONSE_LENGTH = -1;

    private HttpUtils() {
    }

    public static String getLocalEndpoint(int port) {
        return "http://localhost:" + port;
    }

    public static Map<String, String> extractQueryParams(String query) {
        if (query == null || query.isEmpty()) {
            return Map.of();
        }
        return Arrays.stream(query.split("&"))
                .map(param -> param.split("=", 2))
                .filter(entry -> entry.length > 1 && !entry[1].isEmpty())
                .collect(Collectors.toMap(
                        entry -> entry[0],
                        entry -> entry[1],
                        (existing, replacement) -> replacement
                ));
    }

    public static void sendArray(HttpExchange exchange, byte[] responseBody) throws IOException {
        exchange.sendResponseHeaders(HttpCodes.OK, responseBody.length);
        exchange.getResponseBody().write(responseBody);
    }

    @SuppressWarnings("PMD.UseTryWithResources")
    public static List<Integer> findFreePorts(int count) throws IOException {
        List<ServerSocket> sockets = new ArrayList<>(count);
        try {
            for (int i = 0; i < count; i++) {
                var socket = new ServerSocket(0);
                socket.setReuseAddress(true);
                sockets.add(socket);
            }
            return sockets.stream()
                    .map(ServerSocket::getLocalPort)
                    .toList();
        } finally {
            for (ServerSocket socket : sockets) {
                socket.close();
            }
        }
    }
}
