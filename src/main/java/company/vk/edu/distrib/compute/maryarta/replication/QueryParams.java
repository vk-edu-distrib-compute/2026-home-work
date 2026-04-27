package company.vk.edu.distrib.compute.maryarta.replication;

final class QueryParams {
    private QueryParams() {
    }

    static String parseId(String query) {
        if (query == null || query.isBlank()) {
            throw new IllegalArgumentException("Bad query");
        }
        for (String part : query.split("&")) {
            String[] pair = part.split("=", 2);
            if (pair.length == 2 && "id".equals(pair[0])) {
                if (pair[1].isBlank()) {
                    throw new IllegalArgumentException("Blank id");
                }
                return pair[1];
            }
        }
        throw new IllegalArgumentException("Missing id");
    }

    static int parseAck(String query) {
        if (query == null || query.isBlank()) {
            return 1;
        }
        for (String part : query.split("&")) {
            String[] pair = part.split("=", 2);

            if (pair.length == 2 && "ack".equals(pair[0])) {
                return Integer.parseInt(pair[1]);
            }
        }
        return 1;
    }
}
