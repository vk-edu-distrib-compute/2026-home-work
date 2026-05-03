package company.vk.edu.distrib.compute.lillymega;

final class LillymegaRequestParser {
    private static final String ID_PARAMETER = "id";
    private static final String ACK_PARAMETER = "ack";

    LillymegaRequestParameters parse(String query) {
        if (query == null || query.isEmpty()) {
            return null;
        }

        String id = null;
        Integer ack = null;
        for (String parameter : query.split("&")) {
            String[] parts = parameter.split("=", 2);
            if (parts.length != 2) {
                return null;
            }

            if (ID_PARAMETER.equals(parts[0])) {
                id = parts[1];
            } else if (ACK_PARAMETER.equals(parts[0])) {
                ack = Integer.parseInt(parts[1]);
            }
        }

        return new LillymegaRequestParameters(id, ack == null ? 1 : ack);
    }
}
