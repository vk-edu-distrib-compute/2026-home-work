package company.vk.edu.distrib.compute.andeco.election;

public final class LocalisationUtils {
    private LocalisationUtils() {
        //убрать конструктор для utils
    }

    public static String roleRu(NodeRole role) {
        return switch (role) {
            case LEADER -> "лидер";
            case SLAVE -> "slave";
            case DOWN -> "недоступен";
        };
    }
}
