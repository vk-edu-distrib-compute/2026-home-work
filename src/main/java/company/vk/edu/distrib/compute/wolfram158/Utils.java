package company.vk.edu.distrib.compute.wolfram158;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

public final class Utils {
    public static final int GRPC_DIFF = 2026;

    private Utils() {

    }

    public static Map<String, List<String>> extractQueryParams(final String query) {
        if (query == null || query.isEmpty()) {
            return Collections.emptyMap();
        }
        return Arrays
                .stream(query.split("&"))
                .map(pair -> pair.split("=", 2))
                .collect(
                        Collectors.toMap(
                                parts -> parts[0],
                                parts -> parts.length == 2 ? List.of(parts[1]) : Collections.emptyList(),
                                (currentList, newSingleList) -> {
                                    currentList.add(newSingleList.getFirst());
                                    return currentList;
                                }
                        )
                );
    }

    public static void assertNotNulls(Object... objects) {
        if (Arrays.stream(objects).anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException();
        }
    }

    public static int extractPort(String endpoint) {
        return URI.create(endpoint).getPort();
    }

    public static List<String> mapToLocalhostEndpoints(List<Integer> ports) {
        return ports.stream().map(Utils::mapToLocalhostEndpoint).toList();
    }

    public static String mapToLocalhostEndpoint(int port) {
        return "http://localhost:" + port;
    }

    public static String mapToLocalhostGrpcEndpoint(int port) {
        return "localhost:" + (port + GRPC_DIFF);
    }
}
