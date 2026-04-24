package company.vk.edu.distrib.compute.artsobol.impl;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

final class EntityRequest {
    private static final String ID_PARAM = "id";
    private static final String ACK_PARAM = "ack";
    private static final int DEFAULT_ACK = 1;

    private final String id;
    private final int ack;

    private EntityRequest(String id, int ack) {
        this.id = id;
        this.ack = ack;
    }

    static EntityRequest parse(String rawQuery) {
        if (rawQuery == null || rawQuery.isEmpty()) {
            throw new IllegalArgumentException("Missing query");
        }

        Map<String, String> params = new ConcurrentHashMap<>();
        for (String part : rawQuery.split("&")) {
            int separator = part.indexOf('=');
            if (separator <= 0) {
                throw new IllegalArgumentException("Malformed query parameter");
            }
            String name = decode(part.substring(0, separator));
            String value = decode(part.substring(separator + 1));
            params.put(name, value);
        }

        String id = params.get(ID_PARAM);
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("Missing id");
        }

        String rawAck = params.get(ACK_PARAM);
        if (rawAck == null) {
            return new EntityRequest(id, DEFAULT_ACK);
        }
        if (rawAck.isEmpty()) {
            throw new IllegalArgumentException("Empty ack");
        }
        return new EntityRequest(id, Integer.parseInt(rawAck));
    }

    private static String decode(String value) {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    String id() {
        return id;
    }

    int ack() {
        return ack;
    }
}
