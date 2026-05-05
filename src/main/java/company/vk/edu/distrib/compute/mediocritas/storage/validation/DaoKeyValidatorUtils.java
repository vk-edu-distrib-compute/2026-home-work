package company.vk.edu.distrib.compute.mediocritas.storage.validation;

import ch.qos.logback.core.util.StringUtil;

public final class DaoKeyValidatorUtils {

    private DaoKeyValidatorUtils() {

    }

    public static void validateKey(String key) {
        if (!StringUtil.notNullNorEmpty(key)) {
            throw new IllegalArgumentException(String.format("Key %s is not null or empty", key));
        }
    }

}
