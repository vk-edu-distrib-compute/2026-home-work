package company.vk.edu.distrib.compute.expanse.context;

import company.vk.edu.distrib.compute.expanse.exception.BeanInitializationException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class AppContextUtils {
    private static final Map<String, Object> CONTEXT = new ConcurrentHashMap<>();

    private AppContextUtils() {
    }

    public static <T> T getBean(Class<T> clazz) {
        if (CONTEXT.containsKey(clazz.getSimpleName())) {
            return clazz.cast(CONTEXT.get(clazz.getSimpleName()));
        }
        T bean;

        try {
            bean = clazz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new BeanInitializationException(
                    String.format("Failed to initialize bean %s with error: %s",
                            clazz.getSimpleName(), e.getMessage()), e
            );
        }

        CONTEXT.put(clazz.getSimpleName(), bean);
        return bean;
    }
}
