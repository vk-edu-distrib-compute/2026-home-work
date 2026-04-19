package company.vk.edu.distrib.compute.gavrilova_ekaterina.replication;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class ReplicaClient {

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final String selfUrl;
    private final Dao<byte[]> dao;

    public ReplicaClient(String selfUrl, Dao<byte[]> dao) {
        this.selfUrl = selfUrl;
        this.dao = dao;
    }

    public ReplicaResponse get(String node, String id) {
        if (node.equals(selfUrl)) {
            return localGet(id);
        }
        return sendRemoteRequest(node, "GET", id, null);
    }

    public ReplicaResponse put(String node, String id, byte[] body) {
        if (node.equals(selfUrl)) {
            return localPut(id, body);
        }
        return sendRemoteRequest(node, "PUT", id, body);
    }

    public ReplicaResponse delete(String node, String id) {
        if (node.equals(selfUrl)) {
            return localDelete(id);
        }
        return sendRemoteRequest(node, "DELETE", id, null);
    }

    private ReplicaResponse localGet(String id) {
        try {
            byte[] val = dao.get(id);
            return new ReplicaResponse(200, val);
        } catch (Exception e) {
            return new ReplicaResponse(404, null);
        }
    }

    private ReplicaResponse localPut(String id, byte[] body) {
        try {
            dao.upsert(id, body);
            return new ReplicaResponse(201, null);
        } catch (Exception e) {
            return new ReplicaResponse(503, null);
        }
    }

    private ReplicaResponse localDelete(String id) {
        try {
            dao.delete(id);
            return new ReplicaResponse(202, null);
        } catch (Exception e) {
            return new ReplicaResponse(503, null);
        }
    }

    private ReplicaResponse sendRemoteRequest(String node, String method, String id, byte[] body) {
        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .uri(URI.create(node + "/v0/entity?id=" + id))
                    .timeout(Duration.ofSeconds(1))
                    .header("X-Internal", "true");

            switch (method) {
                case "GET" -> builder.GET();
                case "PUT" -> builder.PUT(HttpRequest.BodyPublishers.ofByteArray(body));
                case "DELETE" -> builder.DELETE();
                default -> throw new IllegalArgumentException("Unsupported method: " + method);
            }

            HttpResponse<byte[]> response = httpClient.send(
                    builder.build(),
                    HttpResponse.BodyHandlers.ofByteArray()
            );

            return new ReplicaResponse(response.statusCode(), response.body());

        } catch (IOException | InterruptedException e) {
            return new ReplicaResponse(503, null);
        }
    }

}
