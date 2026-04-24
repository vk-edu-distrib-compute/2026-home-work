package company.vk.edu.distrib.compute.artsobol.impl;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

final class EntityRequest {
    private static final String ID_PARAM = "id";
    private static final String ACK_PARAM = "ack";
    private static final int DEFAULT_ACK = 1;

    private final String entityId;
    private final int ackValue;

    private EntityRequest(String entityId, int ackValue) {
        this.entityId = entityId;
        this.ackValue = ackValue;
    }

    static EntityRequest parse(String rawQuery) {
        if (rawQuery == null || rawQuery.isEmpty()) {
            throw new IllegalArgumentException("Missing query");
        }
        Map<String, String> params = parseParams(rawQuery);
        return new EntityRequest(parseId(params), parseAck(params));
    }

    private static Map<String, String> parseParams(String rawQuery) {
        Map<String, String> params = new HashMap<>();
        for (String part : rawQuery.split("&")) {
            int separator = part.indexOf('=');
            if (separator <= 0) {
                throw new IllegalArgumentException("Malformed query parameter");
            }
            String name = decode(part.substring(0, separator));
            String value = decode(part.substring(separator + 1));
            params.put(name, value);
        }
        return params;
    }

    private static String parseId(Map<String, String> params) {
        String id = params.get(ID_PARAM);
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("Missing id");
        }
        return id;
    }

    private static int parseAck(Map<String, String> params) {
        String rawAck = params.get(ACK_PARAM);
        if (rawAck == null) {
            return DEFAULT_ACK;
        }
        if (rawAck.isEmpty()) {
            throw new IllegalArgumentException("Empty ack");
        }
        return Integer.parseInt(rawAck);
    }

    private static String decode(String value) {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    String id() {
        return entityId;
    }

    int ack() {
        return ackValue;
    }
}
