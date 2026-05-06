package company.vk.edu.distrib.compute.maryarta.replication;

import company.vk.edu.distrib.compute.maryarta.H2Dao;

import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class ReplicaClient {
    private static final String INTERNAL_REPLICATION_HEADER = "Internal-Replication";
    private static final int HTTP_CREATED = 201;
    private static final int HTTP_DELETED = 202;
    private static final int HTTP_NOT_FOUND = 404;
    private final HttpClient httpClient;
    private final String selfEndpoint;
    private final H2Dao localDao;

    public ReplicaClient(HttpClient httpClient, String selfEndpoint, H2Dao localDao) {
        this.httpClient = httpClient;
        this.selfEndpoint = selfEndpoint;
        this.localDao = localDao;
    }

    public boolean put(String endpoint, String key, byte[] value, long version) throws IOException {
        if (endpoint.equals(selfEndpoint)) {
            localDao.upsert(key, value, version, false);
            return true;
        }
        URI uri = URI.create(endpoint + "/v0/entity?id=" + key);
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(byteStream)) {
            out.writeLong(version);
            out.writeBoolean(false);
            if (value == null) {
                out.writeInt(-1);
            } else {
                out.writeInt(value.length);
                out.write(value);
            }
        }
        byte[] requestBody = byteStream.toByteArray();
        HttpRequest request = HttpRequest.newBuilder(uri)
                .header(INTERNAL_REPLICATION_HEADER, "true")
                .PUT(HttpRequest.BodyPublishers.ofByteArray(requestBody))
                .build();
        try {
            HttpResponse<byte[]> response = httpClient.send(
                    request,
                    HttpResponse.BodyHandlers.ofByteArray()
            );
            return response.statusCode() == HTTP_CREATED;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Replica PUT request was interrupted", e);
        }
    }

    public StoredRecord get(String endpoint, String key) throws IOException {
        if (endpoint.equals(selfEndpoint)) {
            return localDao.getRecord(key);
        }
        URI uri = URI.create(endpoint + "/v0/entity?id=" + key);
        HttpRequest request = HttpRequest.newBuilder(uri)
                .header(INTERNAL_REPLICATION_HEADER, "true")
                .GET()
                .build();
        try {
            HttpResponse<byte[]> response = httpClient.send(
                    request,
                    HttpResponse.BodyHandlers.ofByteArray()
            );

            if (response.statusCode() == HTTP_NOT_FOUND) {
                return null;
            }

            return deserializeRecord(response.body());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Replica GET request was interrupted", e);
        }
    }

    private static StoredRecord deserializeRecord(byte[] bytes) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            long version = in.readLong();
            boolean deleted = in.readBoolean();
            int dataLength = in.readInt();
            byte[] data = null;
            if (dataLength >= 0) {
                data = in.readNBytes(dataLength);
                if (data.length != dataLength) {
                    throw new IOException("Corrupted record body");
                }
            }
            return new StoredRecord(data, version, deleted);
        }
    }

    public boolean delete(String endpoint, String key, long version) throws IOException {
        if (endpoint.equals(selfEndpoint)) {
            localDao.delete(key, version);
            return true;
        }
        URI uri = URI.create(endpoint + "/v0/entity?id=" + key);
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(byteStream)) {
            out.writeLong(version);
            out.writeBoolean(true);
            out.writeInt(-1);
        }

        byte[] requestBody = byteStream.toByteArray();
        HttpRequest request = HttpRequest.newBuilder(uri)
                .header(INTERNAL_REPLICATION_HEADER, "true")
                .method("DELETE", HttpRequest.BodyPublishers.ofByteArray(requestBody))
                .build();
        try {
            HttpResponse<byte[]> response = httpClient.send(
                    request,
                    HttpResponse.BodyHandlers.ofByteArray()
            );
            return response.statusCode() == HTTP_DELETED;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Replica DELETE request was interrupted", e);
        }
    }

}
