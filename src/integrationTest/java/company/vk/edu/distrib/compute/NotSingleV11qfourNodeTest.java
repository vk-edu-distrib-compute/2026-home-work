package company.vk.edu.distrib.compute;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.v11qfour.cluster.V11qfourKVClusterFactory;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NotSingleV11qfourNodeTest {
    private static KVCluster cluster;
    private final HttpClient client = HttpClient.newHttpClient();

    @BeforeAll
    static void setup() throws IOException {
        // Создаем кластер на портах 8090 и 8091 (не используем 8080, чтобы не конфликтовать)
        cluster = new V11qfourKVClusterFactory().create(List.of(8090, 8091));
        cluster.start();
    }

    @AfterAll
    static void teardown() {
        cluster.stop();
    }

    @Test
    void testPutAndGetAcrossCluster() throws Exception {
        String key = "testKey";
        String value = "hello-world";

        // 1. PUT на одну ноду
        HttpRequest putRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8090/v0/entity?id=" + key))
                .PUT(HttpRequest.BodyPublishers.ofString(value))
                .build();
        client.send(putRequest, HttpResponse.BodyHandlers.discarding());

        // 2. GET с другой ноды (чтобы проверить проксирование)
        HttpRequest getRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8091/v0/entity?id=" + key))
                .GET()
                .build();
        HttpResponse<String> response = client.send(getRequest, HttpResponse.BodyHandlers.ofString());

        // 3. Проверяем, что получили то же самое
        assertEquals(200, response.statusCode());
        assertEquals(value, response.body());
    }
}
