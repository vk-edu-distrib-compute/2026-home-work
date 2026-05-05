package company.vk.edu.distrib.compute.andeco.election;

public final class LocalisationUtils {
    private LocalisationUtils() {
    }

    public static String roleRu(NodeRole role) {
        return switch (role) {
            case LEADER -> "лидер";
            case SLAVE -> "ведомый";
            case DOWN -> "недоступен";
        };
    }
}
