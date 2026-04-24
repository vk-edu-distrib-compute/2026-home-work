package company.vk.edu.distrib.compute.ce_fello;

import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.ReplicatedService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class CeFelloReplicationTest {
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
    private static final Object CONFIG_LOCK = new Object();
    private static final String FACTOR_PROPERTY = "ce_fello.replication.factor";
    private static final String NODES_PROPERTY = "ce_fello.replication.nodes";
    private static final Duration TIMEOUT = Duration.ofSeconds(2);
    private static final int REPLICATION_FACTOR = 3;
    private static final int TOTAL_REPLICAS = 6;

    @AfterAll
    static void afterAll() {
        HTTP_CLIENT.close();
    }

    @Test
    void serviceSupportsReplicationAndRejectsBadAck() throws Exception {
        assertDoesNotThrow(() -> withService(service -> {
            assertInstanceOf(ReplicatedService.class, service);
            assertEquals(400, get(service.port(), "bad-ack-key", service.numberOfReplicas() + 1).statusCode());
        }));
    }

    @Test
    void quorumWithFaultyNodes() throws Exception {
        assertDoesNotThrow(() -> withService(service -> {
            service.disableReplica(0);
            int n = service.numberOfReplicas();
            int writeAck = n - 1;
            int readAck = n - 2;

            assertEquals(
                    201,
                    upsert(service.port(), "k3", "v3".getBytes(StandardCharsets.UTF_8), writeAck).statusCode()
            );
            service.disableReplica(1);

            HttpResponse<byte[]> response = get(service.port(), "k3", readAck);
            assertEquals(200, response.statusCode());
            assertEquals("v3", new String(response.body(), StandardCharsets.UTF_8));
        }));
    }

    @Test
    void returnsFreshestValueWhenReplicasDiverge() throws Exception {
        assertDoesNotThrow(() -> withService(service -> {
            service.disableReplica(0);
            int n = service.numberOfReplicas();

            assertEquals(201, upsert(service.port(), "k4", "v4_0".getBytes(StandardCharsets.UTF_8), n).statusCode());

            service.disableReplica(0);
            service.disableReplica(1);
            assertEquals(
                    201,
                    upsert(service.port(), "k4", "v4_1".getBytes(StandardCharsets.UTF_8), n - 2).statusCode()
            );

            service.enableReplica(0);
            service.enableReplica(1);

            HttpResponse<byte[]> response = get(service.port(), "k4", n);
            assertEquals(200, response.statusCode());
            assertEquals("v4_1", new String(response.body(), StandardCharsets.UTF_8));
        }));
    }

    @Test
    void returnsFiveHundredWhenNotEnoughReplicasForReadOrWrite() throws Exception {
        assertDoesNotThrow(() -> withService(service -> {
            int n = service.numberOfReplicas();

            service.disableReplica(0);
            assertEquals(500, upsert(service.port(), "k5", "v5".getBytes(StandardCharsets.UTF_8), n).statusCode());

            service.enableReplica(0);
            service.disableReplica(0);
            assertEquals(201, upsert(service.port(), "k6", "v6".getBytes(StandardCharsets.UTF_8), n - 1).statusCode());
            service.disableReplica(1);

            assertEquals(500, get(service.port(), "k6", n).statusCode());
        }));
    }

    @Test
    void hidesDeletedValueAndUpdatesStats() throws Exception {
        assertDoesNotThrow(() -> withService(service -> {
            int n = service.numberOfReplicas();

            assertEquals(201, upsert(service.port(), "k7", "v7".getBytes(StandardCharsets.UTF_8), n).statusCode());
            service.disableReplica(1);
            service.disableReplica(2);
            int writeAck = n - 2;
            assertEquals(202, delete(service.port(), "k7", writeAck).statusCode());

            service.enableReplica(1);
            service.enableReplica(2);
            int readAck = n - writeAck + 1;
            assertEquals(404, get(service.port(), "k7", readAck).statusCode());

            assertEquals(200, getStats(service.port(), 0).statusCode());
            assertEquals(200, getAccessStats(service.port(), 0).statusCode());
        }));
    }

    private static void withService(ServiceAction action) throws Exception {
        synchronized (CONFIG_LOCK) {
            String previousFactor = System.getProperty(FACTOR_PROPERTY);
            String previousNodes = System.getProperty(NODES_PROPERTY);
            System.setProperty(FACTOR_PROPERTY, String.valueOf(REPLICATION_FACTOR));
            System.setProperty(NODES_PROPERTY, String.valueOf(TOTAL_REPLICAS));
            try {
                KVServiceFactory factory = new CeFelloKVServiceFactory();
                ReplicatedService service = assertInstanceOf(
                        ReplicatedService.class,
                        factory.create(randomPort())
                );
                service.start();
                try {
                    action.perform(service);
                } finally {
                    service.stop();
                }
            } finally {
                restoreProperty(FACTOR_PROPERTY, previousFactor);
                restoreProperty(NODES_PROPERTY, previousNodes);
            }
        }
    }

    private static void restoreProperty(String key, String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }

    private static HttpResponse<byte[]> get(int port, String key, Integer ack)
            throws IOException, InterruptedException {
        return HTTP_CLIENT.send(
                HttpRequest.newBuilder()
                        .GET()
                        .uri(URI.create(url(port, key, ack)))
                        .timeout(TIMEOUT)
                        .build(),
                HttpResponse.BodyHandlers.ofByteArray()
        );
    }

    private static HttpResponse<Void> upsert(int port, String key, byte[] value, Integer ack)
            throws IOException, InterruptedException {
        return HTTP_CLIENT.send(
                HttpRequest.newBuilder()
                        .PUT(HttpRequest.BodyPublishers.ofByteArray(value))
                        .uri(URI.create(url(port, key, ack)))
                        .timeout(TIMEOUT)
                        .build(),
                HttpResponse.BodyHandlers.discarding()
        );
    }

    private static HttpResponse<Void> delete(int port, String key, Integer ack)
            throws IOException, InterruptedException {
        return HTTP_CLIENT.send(
                HttpRequest.newBuilder()
                        .DELETE()
                        .uri(URI.create(url(port, key, ack)))
                        .timeout(TIMEOUT)
                        .build(),
                HttpResponse.BodyHandlers.discarding()
        );
    }

    private static HttpResponse<byte[]> getStats(int port, int replicaId) throws IOException, InterruptedException {
        return HTTP_CLIENT.send(
                HttpRequest.newBuilder()
                        .GET()
                        .uri(URI.create("http://localhost:" + port + "/stats/replica/" + replicaId))
                        .timeout(TIMEOUT)
                        .build(),
                HttpResponse.BodyHandlers.ofByteArray()
        );
    }

    private static HttpResponse<byte[]> getAccessStats(int port, int replicaId)
            throws IOException, InterruptedException {
        return HTTP_CLIENT.send(
                HttpRequest.newBuilder()
                        .GET()
                        .uri(URI.create("http://localhost:" + port + "/stats/replica/" + replicaId + "/access"))
                        .timeout(TIMEOUT)
                        .build(),
                HttpResponse.BodyHandlers.ofByteArray()
        );
    }

    private static String url(int port, String key, Integer ack) {
        String url = "http://localhost:" + port + "/v0/entity?id=" + key;
        return ack == null ? url : url + "&ack=" + ack;
    }

    private static int randomPort() {
        for (int i = 0; i < 100_000; i++) {
            int port = ThreadLocalRandom.current().nextInt(10_000, 60_000);
            if (isTcpPortAvailable(port)) {
                return port;
            }
        }
        throw new IllegalStateException("Can't find available port");
    }

    private static boolean isTcpPortAvailable(int port) {
        try (ServerSocket serverSocket = new ServerSocket()) {
            serverSocket.setReuseAddress(false);
            serverSocket.bind(new InetSocketAddress(InetAddress.getByName("localhost"), port), 1);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @FunctionalInterface
    private interface ServiceAction {
        void perform(ReplicatedService service) throws Exception;
    }
}
