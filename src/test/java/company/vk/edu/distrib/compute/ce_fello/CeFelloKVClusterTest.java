package company.vk.edu.distrib.compute.ce_fello;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class CeFelloKVClusterTest {
    private static final String LOCALHOST_PREFIX = "http://localhost:";
    private static final String ENTITY_PREFIX = "/v0/entity?id=";
    private static final int HTTP_OK_STATUS = 200;
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    @TempDir
    Path tempDirectory;

    @AfterAll
    static void afterAll() {
        HTTP_CLIENT.close();
    }

    @Test
    void normalizesEndpointOrder() {
        CeFelloKVCluster cluster = new CeFelloKVCluster(
                List.of(18081, 18080),
                tempDirectory,
                CeFelloDistributionMode.RENDEZVOUS
        );

        assertEquals(List.of(LOCALHOST_PREFIX + "18080", LOCALHOST_PREFIX + "18081"), cluster.getEndpoints());
    }

    @Test
    void proxiesCrudAcrossNodes() throws Exception {
        CeFelloKVCluster cluster = newCluster();
        cluster.start();

        try {
            List<String> endpoints = cluster.getEndpoints();
            String key = "cluster-key";
            byte[] value1 = new byte[]{1, 2, 3};
            byte[] value2 = new byte[]{4, 5};

            assertEquals(201, upsert(endpoints.getLast(), key, value1).statusCode());
            assertArrayEquals(value1, get(endpoints.getFirst(), key).body());

            assertEquals(201, upsert(endpoints.getFirst(), key, value2).statusCode());
            for (String endpoint : endpoints) {
                HttpResponse<byte[]> response = get(endpoint, key);
                assertEquals(200, response.statusCode());
                assertArrayEquals(value2, response.body());
            }

            assertEquals(202, delete(endpoints.getLast(), key).statusCode());
            for (String endpoint : endpoints) {
                assertEquals(404, get(endpoint, key).statusCode());
            }
        } finally {
            cluster.stop();
        }
    }

    @Test
    void storesOnlyOnePhysicalCopy() throws Exception {
        List<Integer> ports = randomPorts(2);
        CeFelloKVCluster cluster = new CeFelloKVCluster(ports, tempDirectory, CeFelloDistributionMode.RENDEZVOUS);
        String key = "single-copy-key";
        byte[] value = new byte[]{7, 8, 9};

        cluster.start();
        try {
            assertEquals(201, upsert(cluster.getEndpoints().getFirst(), key, value).statusCode());
        } finally {
            cluster.stop();
        }

        int successCount = 0;
        for (Integer port : ports) {
            Path keyPath = tempDirectory.resolve(String.valueOf(port)).resolve(encodeKey(key));
            if (Files.exists(keyPath)) {
                assertArrayEquals(value, Files.readAllBytes(keyPath));
                successCount++;
            }
        }

        assertEquals(1, successCount);
    }

    @Test
    void restartedSingleEndpointsRevealOnlyOwnerData() throws Exception {
        CeFelloKVCluster cluster = newCluster();
        String key = "distributed-key";
        byte[] value = new byte[]{10, 11, 12};

        cluster.start();
        try {
            assertEquals(201, upsert(cluster.getEndpoints().getLast(), key, value).statusCode());
        } finally {
            cluster.stop();
        }

        int successCount = 0;
        for (String endpoint : cluster.getEndpoints()) {
            cluster.start(endpoint);
            try {
                HttpResponse<byte[]> response = get(endpoint, key);
                if (response.statusCode() == HTTP_OK_STATUS) {
                    assertArrayEquals(value, response.body());
                    successCount++;
                }
            } finally {
                cluster.stop(endpoint);
            }
        }

        assertEquals(1, successCount);
    }

    private CeFelloKVCluster newCluster() {
        return new CeFelloKVCluster(randomPorts(2), tempDirectory, CeFelloDistributionMode.RENDEZVOUS);
    }

    private static HttpResponse<byte[]> get(String endpoint, String key) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(new URI(endpoint + ENTITY_PREFIX + key))
                .timeout(Duration.ofSeconds(2))
                .build();
        return HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofByteArray());
    }

    private static HttpResponse<Void> delete(String endpoint, String key) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .DELETE()
                .uri(new URI(endpoint + ENTITY_PREFIX + key))
                .timeout(Duration.ofSeconds(2))
                .build();
        return HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.discarding());
    }

    private static HttpResponse<Void> upsert(String endpoint, String key, byte[] data) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .PUT(HttpRequest.BodyPublishers.ofByteArray(data))
                .uri(new URI(endpoint + ENTITY_PREFIX + key))
                .timeout(Duration.ofSeconds(2))
                .build();
        return HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.discarding());
    }

    private static List<Integer> randomPorts(int count) {
        Set<Integer> ports = new HashSet<>();
        while (ports.size() < count) {
            ports.add(randomPort());
        }
        return new ArrayList<>(ports);
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

    private static String encodeKey(String key) {
        return Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(key.getBytes(StandardCharsets.UTF_8));
    }
}
