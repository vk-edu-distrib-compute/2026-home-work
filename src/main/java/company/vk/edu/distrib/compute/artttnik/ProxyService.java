package company.vk.edu.distrib.compute.artttnik;

import com.google.protobuf.ByteString;
import company.vk.edu.distrib.compute.artttnik.grpc.InternalKvServiceGrpc;
import company.vk.edu.distrib.compute.artttnik.grpc.ProxyRequest;
import company.vk.edu.distrib.compute.artttnik.grpc.ProxyResponse;
import company.vk.edu.distrib.compute.artttnik.shard.ShardingStrategy;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

final class ProxyService {
    private static final String GRPC_PORT_PARAM = "grpcPort";

    private final String self;
    private final List<String> endpoints;
    private final ShardingStrategy strategy;
    private final Map<String, ManagedChannel> channels;

    ProxyService(String self, List<String> endpoints, ShardingStrategy strategy) {
        this.self = self;
        this.endpoints = endpoints;
        this.strategy = strategy;
        this.channels = new ConcurrentHashMap<>();
    }

    boolean shouldProxy(String internalHeaderValue, String id) {
        return !endpoints.isEmpty()
                && !"true".equals(internalHeaderValue)
                && !self.equals(strategy.resolveOwner(id, endpoints));
    }

    ProxyResult proxy(String method, String id, String rawAck, byte[] body) {
        String owner = strategy.resolveOwner(id, endpoints);
        EndpointInfo endpointInfo = EndpointInfo.from(owner);
        ManagedChannel channel = channels.computeIfAbsent(
                endpointInfo.channelKey(),
                key -> ManagedChannelBuilder
                        .forAddress(endpointInfo.host(), endpointInfo.grpcPort())
                        .usePlaintext()
                        .build()
        );

        ProxyRequest.Builder requestBuilder = ProxyRequest.newBuilder()
                .setMethod(method)
                .setId(id)
                .setBody(ByteString.copyFrom(body));

        if (rawAck != null && !rawAck.isBlank()) {
            requestBuilder.setHasAck(true).setAck(Integer.parseInt(rawAck));
        }

        ProxyResponse response = InternalKvServiceGrpc.newBlockingStub(channel).proxy(requestBuilder.build());
        return new ProxyResult(response.getStatusCode(), response.getBody().toByteArray());
    }

    void close() {
        for (ManagedChannel channel : channels.values()) {
            channel.shutdown();
            try {
                if (!channel.awaitTermination(2, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                channel.shutdownNow();
            }
        }
    }

    private record EndpointInfo(String host, int grpcPort) {

        private static EndpointInfo from(String endpoint) {
            URI uri = URI.create(endpoint);
            String host = uri.getHost();
            if (host == null || host.isBlank()) {
                throw new IllegalArgumentException("Endpoint host is empty: " + endpoint);
            }
            int grpcPort = extractGrpcPort(uri);
            return new EndpointInfo(host, grpcPort);
        }

        private static int extractGrpcPort(URI uri) {
            Map<String, String> queryParams = parseQueryParams(uri.getRawQuery());
            String grpcPort = queryParams.get(GRPC_PORT_PARAM);
            if (grpcPort == null || grpcPort.isBlank()) {
                return MyReplicatedKVService.defaultGrpcPort(uri.getPort());
            }
            return Integer.parseInt(grpcPort);
        }

        private String channelKey() {
            return host + ":" + grpcPort;
        }
    }

    private static Map<String, String> parseQueryParams(String query) {
        Map<String, String> params = new ConcurrentHashMap<>();
        if (query == null || query.isBlank()) {
            return params;
        }

        for (String pair : query.split("&")) {
            String[] parts = pair.split("=", 2);
            params.put(
                    URLDecoder.decode(parts[0], StandardCharsets.UTF_8),
                    parts.length > 1 ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8) : ""
            );
        }
        return params;
    }
}
