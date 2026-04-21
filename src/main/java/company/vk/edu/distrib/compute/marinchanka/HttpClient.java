package company.vk.edu.distrib.compute.marinchanka;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class HttpClient {
    private static final java.net.http.HttpClient CLIENT = java.net.http.HttpClient.newBuilder()
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

            if (response.statusCode() == 200) {
                return response.body();
            }
            throw new RuntimeException("Key not found or error: " + response.statusCode());
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to proxy GET to " + node, e);
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

            if (response.statusCode() != 201) {
                throw new RuntimeException("Failed to put: " + response.statusCode());
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to proxy PUT to " + node, e);
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

            if (response.statusCode() != 202) {
                throw new RuntimeException("Failed to delete: " + response.statusCode());
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to proxy DELETE to " + node, e);
        }
    }
}
