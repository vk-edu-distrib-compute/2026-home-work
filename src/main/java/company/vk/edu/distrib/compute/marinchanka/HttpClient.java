package company.vk.edu.distrib.compute.marinchanka;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class HttpClient {
    private static final int STATUS_OK = 200;
    private static final int STATUS_CREATED = 201;
    private static final int STATUS_ACCEPTED = 202;

    public static final java.net.http.HttpClient CLIENT = java.net.http.HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    public byte[] get(ClusterNode node, String key) {
        try {
            String url = node.getUrl() + "/v0/entity?id=" + key;
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .timeout(Duration.ofSeconds(5))
                    .build();

            HttpResponse<byte[]> response = CLIENT.send(request,
                    HttpResponse.BodyHandlers.ofByteArray());

            if (response.statusCode() != STATUS_OK) {
                throw new IllegalStateException("Key not found or error: " + response.statusCode());
            }
            return response.body();
        } catch (IOException | InterruptedException e) {
            throw new IllegalStateException("Failed to proxy GET to " + node, e);
        }
    }

    public void put(ClusterNode node, String key, byte[] value) {
        try {
            String url = node.getUrl() + "/v0/entity?id=" + key;
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .PUT(HttpRequest.BodyPublishers.ofByteArray(value))
                    .timeout(Duration.ofSeconds(5))
                    .build();

            HttpResponse<Void> response = CLIENT.send(request,
                    HttpResponse.BodyHandlers.discarding());

            if (response.statusCode() != STATUS_CREATED) {
                throw new IllegalStateException("Failed to put: " + response.statusCode());
            }
        } catch (IOException | InterruptedException e) {
            throw new IllegalStateException("Failed to proxy PUT to " + node, e);
        }
    }

    public void delete(ClusterNode node, String key) {
        try {
            String url = node.getUrl() + "/v0/entity?id=" + key;
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .DELETE()
                    .timeout(Duration.ofSeconds(5))
                    .build();

            HttpResponse<Void> response = CLIENT.send(request,
                    HttpResponse.BodyHandlers.discarding());

            if (response.statusCode() != STATUS_ACCEPTED) {
                throw new IllegalStateException("Failed to delete: " + response.statusCode());
            }
        } catch (IOException | InterruptedException e) {
            throw new IllegalStateException("Failed to proxy DELETE to " + node, e);
        }
    }
}
