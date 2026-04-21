package company.vk.edu.distrib.compute.korjick.http.entity;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.korjick.http.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

public class RendezvousEntityRequestProcessor extends LocalEntityRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(RendezvousEntityRequestProcessor.class);
    private static final Duration PROXY_TIMEOUT = Duration.ofSeconds(2);

    private final String selfEndpoint;
    private final List<String> endpoints;
    private final HttpClient httpClient;

    public RendezvousEntityRequestProcessor(Dao<byte[]> dao, String selfEndpoint, List<String> endpoints) {
        super(dao);
        this.selfEndpoint = selfEndpoint;
        this.endpoints = List.copyOf(endpoints);
        this.httpClient = HttpClient.newHttpClient();
    }

    @Override
    public EntityResponse process(EntityRequest request) throws IOException {
        String destination = resolve(request.id());
        if (!request.internalRequest() && !Objects.equals(selfEndpoint, destination)) {
            return proxy(destination, request);
        }
        return super.process(request);
    }

    private EntityResponse proxy(String destinationEndpoint, EntityRequest request) throws IOException {
        String encodedId = URLEncoder.encode(request.id(), StandardCharsets.UTF_8);
        URI targetUri = URI.create(destinationEndpoint + "/v0/entity?id=" + encodedId);
        HttpRequest.Builder builder = HttpRequest.newBuilder(targetUri)
                .timeout(PROXY_TIMEOUT)
                .header(Constants.INTERNAL_REQUEST_HEADER, Constants.INTERNAL_REQUEST_HEADER_VALUE);

        switch (request.method()) {
            case Constants.HTTP_METHOD_GET -> builder.GET();
            case Constants.HTTP_METHOD_DELETE -> builder.DELETE();
            case Constants.HTTP_METHOD_PUT -> builder.PUT(HttpRequest.BodyPublishers.ofByteArray(request.body()));
            default -> {
                return new EntityResponse(Constants.HTTP_STATUS_METHOD_NOT_ALLOWED, null);
            }
        }

        try {
            HttpResponse<byte[]> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
            return new EntityResponse(response.statusCode(), response.body());
        } catch (InterruptedException e) {
            log.error("Failed to proxy request to {}", destinationEndpoint, e);
            throw new IOException("Interrupted while proxying request", e);
        } catch (IOException e) {
            log.error("Failed to proxy request to {}", destinationEndpoint, e);
            return new EntityResponse(Constants.HTTP_STATUS_BAD_GATEWAY, null);
        }
    }

    private String resolve(String key) {
        String selected = null;
        long bestScore = 0;
        for (String endpoint : endpoints) {
            long score = Objects.hash(key, endpoint);
            if (selected == null || Long.compareUnsigned(score, bestScore) > 0) {
                bestScore = score;
                selected = endpoint;
            }
        }
        return selected;
    }
}
