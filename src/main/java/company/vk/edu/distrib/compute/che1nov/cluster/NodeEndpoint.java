package company.vk.edu.distrib.compute.che1nov.cluster;

import java.net.URI;
import java.util.Locale;

public record NodeEndpoint(
        String endpoint,
        String httpBase,
        String grpcHost,
        int grpcPort
) {
    private static final String PARAM_GRPC_PORT = "grpcPort";

    public static NodeEndpoint parse(String endpoint) {
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalArgumentException("endpoint must not be null or blank");
        }

        URI uri = URI.create(endpoint);
        String host = uri.getHost();
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("invalid endpoint host: " + endpoint);
        }

        int grpcPort = parseGrpcPort(uri.getRawQuery());
        if (grpcPort <= 0 || grpcPort > 65_535) {
            throw new IllegalArgumentException("invalid grpcPort in endpoint: " + endpoint);
        }

        int httpPort = uri.getPort();
        String scheme = uri.getScheme();
        if (scheme == null || scheme.isBlank()) {
            scheme = "http";
        }
        String httpBase = scheme.toLowerCase(Locale.ROOT) + "://" + host + ":" + httpPort;
        return new NodeEndpoint(endpoint, httpBase, host, grpcPort);
    }

    private static int parseGrpcPort(String rawQuery) {
        if (rawQuery == null || rawQuery.isBlank()) {
            throw new IllegalArgumentException("missing grpcPort query parameter");
        }
        for (String pair : rawQuery.split("&")) {
            String[] parts = pair.split("=", 2);
            if (parts.length == 2 && PARAM_GRPC_PORT.equals(parts[0])) {
                return Integer.parseInt(parts[1]);
            }
        }
        throw new IllegalArgumentException("missing grpcPort query parameter");
    }
}
