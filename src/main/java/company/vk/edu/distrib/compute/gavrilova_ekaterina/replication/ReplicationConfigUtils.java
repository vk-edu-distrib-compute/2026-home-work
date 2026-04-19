package company.vk.edu.distrib.compute.gavrilova_ekaterina.replication;

public final class ReplicationConfigUtils {

    private static final int DEFAULT_REPLICATION_FACTOR = 3;
    private static final String ENV_KEY = "REPLICATION_FACTOR";

    private ReplicationConfigUtils() {
    }

    public static int getReplicationFactor() {
        String value = System.getenv(ENV_KEY);

        if (value == null || value.isBlank()) {
            return DEFAULT_REPLICATION_FACTOR;
        }

        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid replication factor env value: " + value, e);
        }
    }

}
