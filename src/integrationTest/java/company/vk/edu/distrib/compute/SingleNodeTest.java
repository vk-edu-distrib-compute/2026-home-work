package company.vk.edu.distrib.compute;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for single node {@link KVService} API
 *
 */
public class SingleNodeTest extends TestBase {
    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private static String endpoint;
    private static KVService storage;

    @BeforeAll
    public static void beforeAll() throws IOException {
        final int port = randomPort();
        endpoint = endpoint(port);
        storage = KVServiceFactory.create(port);
        storage.start();
    }

    @AfterAll
    public static void afterAll() {
        storage.stop();
        httpClient.close();
    }


    private String url(final String id) {
        return endpoint + "/v0/entity?id=" + id;
    }

    private HttpResponse<byte[]> get(String key) throws IOException, URISyntaxException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
            .GET()
            .uri(new URI(url(key)))
            .timeout(Duration.ofSeconds(2))
            .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
    }

    private HttpResponse<Void> delete(String key) throws IOException, URISyntaxException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
            .DELETE()
            .uri(new URI(url(key)))
            .timeout(Duration.ofSeconds(2))
            .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.discarding());
    }

    private HttpResponse<Void> upsert(String key, byte[] data) throws IOException, URISyntaxException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
            .PUT(HttpRequest.BodyPublishers.ofByteArray(data))
            .uri(new URI(url(key)))
            .timeout(Duration.ofSeconds(2))
            .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.discarding());
    }

    @Test
    public void emptyKey() throws Exception {
        assertEquals(400, get("").statusCode());
        assertEquals(400, delete("").statusCode());
        assertEquals(400, upsert("", new byte[]{0}).statusCode());
    }

    @Test
    public void badRequest() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
            .GET()
            .uri(new URI(url("/abracadabra")))
            .timeout(Duration.ofSeconds(2))
            .build();
        assertEquals(404, httpClient.send(request, HttpResponse.BodyHandlers.discarding()).statusCode());
    }

    @Test
    public void getAbsent() throws Exception {
        assertEquals(
                404,
                get("absent").statusCode());
    }

    @Test
    public void deleteAbsent() throws Exception {
        assertEquals(202, delete("absent").statusCode());
    }

    @Test
    public void insert() throws Exception {
        String key = randomKey();
        byte[] value = randomValue();

        // Insert
        assertEquals(201, upsert(key, value).statusCode());

        // Check
        final HttpResponse<byte[]> response = get(key);
        assertEquals(200, response.statusCode());
        assertArrayEquals(value, response.body());
    }

    @Test
    public void insertEmpty() throws Exception {
        final String key = randomKey();
        final byte[] value = new byte[0];

        // Insert
        assertEquals(201, upsert(key, value).statusCode());

        // Check
        final HttpResponse<byte[]> response = get(key);
        assertEquals(200, response.statusCode());
        assertArrayEquals(value, response.body());
    }

    @Test
    public void lifecycle2keys() throws Exception {
        final String key1 = randomKey();
        final byte[] value1 = randomValue();
        final String key2 = randomKey();
        final byte[] value2 = randomValue();

        // Insert 1
        assertEquals(201, upsert(key1, value1).statusCode());

        // Check
        assertArrayEquals(value1, get(key1).body());

        // Insert 2
        assertEquals(201, upsert(key2, value2).statusCode());

        // Check
        assertArrayEquals(value1, get(key1).body());
        assertArrayEquals(value2, get(key2).body());

        // Delete 1
        assertEquals(202, delete(key1).statusCode());

        // Check
        assertEquals(404, get(key1).statusCode());
        assertArrayEquals(value2, get(key2).body());

        // Delete 2
        assertEquals(202, delete(key2).statusCode());

        // Check
        assertEquals(404, get(key2).statusCode());
    }

    @Test
    public void upsert() throws Exception {
        final String key = randomKey();
        final byte[] value1 = randomValue();
        final byte[] value2 = randomValue();

        // Insert value1
        assertEquals(201, upsert(key, value1).statusCode());

        // Insert value2
        assertEquals(201, upsert(key, value2).statusCode());

        // Check value 2
        final HttpResponse<byte[]> response = get(key);
        assertEquals(200, response.statusCode());
        assertArrayEquals(value2, response.body());
    }

    @Test
    public void upsertEmpty() throws Exception {
        final String key = randomKey();
        final byte[] value = randomValue();
        final byte[] empty = new byte[0];

        // Insert value
        assertEquals(201, upsert(key, value).statusCode());

        // Insert empty
        assertEquals(201, upsert(key, empty).statusCode());

        // Check empty
        final HttpResponse<byte[]> response = get(key);
        assertEquals(200, response.statusCode());
        assertArrayEquals(empty, response.body());
    }

    @Test
    public void delete() throws Exception {
        final String key = randomKey();
        final byte[] value = randomValue();

        // Insert
        assertEquals(201, upsert(key, value).statusCode());

        // Delete
        assertEquals(202, delete(key).statusCode());

        // Check
        assertEquals(404, get(key).statusCode());
    }
}
