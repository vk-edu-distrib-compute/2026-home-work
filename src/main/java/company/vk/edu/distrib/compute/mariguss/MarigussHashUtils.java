package company.vk.edu.distrib.compute.mariguss;

import java.util.List;

// 1. Добавляем final и меняем имя на Utils (согласно ClassNamingConventions)
public final class MarigussHashUtils {

    // 2. Добавляем приватный конструктор (согласно UseUtilityClass)
    private MarigussHashUtils() {
        throw new UnsupportedOperationException("Utility class");
    }

    public static String getNodeEndpoint(String key, List<String> endpoints) {
        // 3. Добавляем фигурные скобки (согласно ControlStatementBraces)
        if (endpoints == null || endpoints.isEmpty()) {
            return null;
        }
        
        String bestNode = null;
        int maxHash = Integer.MIN_VALUE;

        for (String node : endpoints) {
            int hash = (key + node).hashCode();
            if (hash > maxHash) {
                maxHash = hash;
                bestNode = node;
            }
        }
        return bestNode;
    }
}
