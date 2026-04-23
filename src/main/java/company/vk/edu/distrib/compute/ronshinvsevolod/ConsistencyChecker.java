package company.vk.edu.distrib.compute.ronshinvsevolod;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public final class ConsistencyChecker {

    private static final Logger LOG = Logger.getLogger(ConsistencyChecker.class.getName());
    private static final String BASE_URL = "http://localhost:8080";
    private static final String KEY = "k";
    private static final int ITERATIONS = 1000;
    private static final int HTTP_OK = 200;
    private static final int HTTP_CREATED = 201;
    private static final int TIMEOUT_SECONDS = 5;

    private ConsistencyChecker() {
    }

    public static void main(final String[] args) throws InterruptedException {
        int ackW = args.length >= 1 ? Integer.parseInt(args[0]) : 2;
        int ackR = args.length >= 2 ? Integer.parseInt(args[1]) : 2;

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(TIMEOUT_SECONDS))
                .build();

        AtomicInteger maxWritten = new AtomicInteger(0);
        AtomicInteger reads = new AtomicInteger(0);
        AtomicInteger stale = new AtomicInteger(0);

        Thread writer = makeWriter(client, ackW, maxWritten);
        Thread reader = makeReader(client, ackR, maxWritten, reads, stale);

        writer.start();
        reader.start();
        writer.join();
        reader.join();

        printResults(ackW, ackR, reads.get(), stale.get());
    }

    private static Thread makeWriter(HttpClient client, int ackW,
                                     AtomicInteger maxWritten) {
        return new Thread(() -> {
            for (int i = 1; i <= ITERATIONS; i++) {
                if (doPut(client, i, ackW)) {
                    maxWritten.set(i);
                }
            }
        });
    }

    private static Thread makeReader(HttpClient client, int ackR,
                                     AtomicInteger maxWritten,
                                     AtomicInteger reads,
                                     AtomicInteger stale) {
        return new Thread(() -> {
            for (int i = 0; i < ITERATIONS; i++) {
                String body = doGet(client, ackR);
                if (body == null || body.isBlank()) {
                    continue;
                }
                try {
                    int val = Integer.parseInt(body.trim());
                    int snapshot = maxWritten.get();
                    reads.incrementAndGet();
                    if (val < snapshot) {
                        stale.incrementAndGet();
                    }
                } catch (NumberFormatException e) {
                    LOG.log(java.util.logging.Level.WARNING, "Unexpected body format", e);
                }
            }
        });
    }

    private static boolean doPut(HttpClient client, int value, int ack) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(BASE_URL + "/v0/entity?id=" + KEY + "&ack=" + ack))
                    .timeout(Duration.ofSeconds(TIMEOUT_SECONDS))
                    .PUT(HttpRequest.BodyPublishers.ofString(String.valueOf(value)))
                    .build();
            return client.send(req, HttpResponse.BodyHandlers.discarding())
                    .statusCode() == HTTP_CREATED;
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static String doGet(HttpClient client, int ack) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(BASE_URL + "/v0/entity?id=" + KEY + "&ack=" + ack))
                    .timeout(Duration.ofSeconds(TIMEOUT_SECONDS))
                    .GET()
                    .build();
            HttpResponse<String> resp = client.send(req,
                    HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() == HTTP_OK) {
                return resp.body();
            }
            return null;
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private static void printResults(int ackW, int ackR, int r, int s) {
        if (LOG.isLoggable(java.util.logging.Level.INFO)) {
            LOG.info("ack_w=" + ackW + "  ack_r=" + ackR);
        }
        if (LOG.isLoggable(java.util.logging.Level.INFO)) {
            LOG.info("GETs: " + r);
        }
        if (LOG.isLoggable(java.util.logging.Level.INFO)) {
            LOG.info("Stale reads: " + s);
        }
        if (r > 0 && LOG.isLoggable(java.util.logging.Level.INFO)) {
            LOG.info(String.format("Consistency: %.2f%%", 100.0 - (s * 100.0 / r)));
        }
    }
}
