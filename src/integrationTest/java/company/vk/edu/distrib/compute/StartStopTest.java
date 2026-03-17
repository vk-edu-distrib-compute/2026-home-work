package company.vk.edu.distrib.compute;

import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Basic init/deinit test for {@link KVService} implementation
 *
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class StartStopTest extends TestBase {
    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private static int port;
    private static KVService storage;

    @BeforeAll
    public static void beforeAll() {
        port = randomPort();
    }

    @AfterAll
    public static void afterAll() {
        httpClient.close();
    }

    private static int status() throws IOException, URISyntaxException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
            .GET()
            .uri(new URI("http://localhost:" + port + "/v0/status"))
            .timeout(Duration.ofSeconds(2))
            .build();
        HttpResponse<Void> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
        return response.statusCode();
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.SECONDS)
    public void create() throws Exception {
        storage = KVServiceFactory.create(port);
        try {
            // Should not respond before start
            status();
        } catch (SocketTimeoutException e) {
            // Do nothing
        }
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.SECONDS)
    public void start() throws Exception {
        storage.start();
        assertEquals(200, status());
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.SECONDS)
    public void stop() throws Exception {
        storage.stop();
        try {
            // Should not respond after stop
            status();
        } catch (SocketTimeoutException e) {
            // Do nothing
        }
    }
}
