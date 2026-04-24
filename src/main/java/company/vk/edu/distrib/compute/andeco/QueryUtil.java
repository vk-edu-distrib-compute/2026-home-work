package company.vk.edu.distrib.compute.andeco;

public final class QueryUtil {
    private static final String ID = "id";
    private static final String ACK = "ack";

    private QueryUtil() {
    }

    public static String extractId(String query) {
        return extractParam(query, ID);
    }

    public static Integer extractAck(String query) {
        String value = extractParam(query, ACK);
        if (value == null) {
            return null;
        }

        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid ack: " + value, e);
        }
    }

    private static String extractParam(String query, String name) {
        if (query == null) {
            return null;
        }

        for (String param : query.split("&")) {
            int idx = param.indexOf('=');
            if (idx > 0 && name.equals(param.substring(0, idx))) {
                return param.substring(idx + 1);
            }
        }
        return null;
    }
}
