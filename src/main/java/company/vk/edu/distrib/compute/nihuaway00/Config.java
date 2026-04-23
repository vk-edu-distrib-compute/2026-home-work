package company.vk.edu.distrib.compute.nihuaway00;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public final class Config {
    private static final Logger log = LoggerFactory.getLogger(Config.class);
    private static final Properties PROPS = load();

    private static final int MIN_REPLICAS_COUNT = 1;

    private Config() {
    }

    static String strategy() {
        return PROPS.getProperty("strategy", "consistent");
    }

    static int replicas() {
        int n = Integer.parseInt(PROPS.getProperty("replicas", "3"));

        if (n < MIN_REPLICAS_COUNT) {
            throw new IllegalArgumentException("replicas must be >= 1");
        }
        return n;
    }

    private static Properties load() {
        try (var is = Config.class.getClassLoader()
                .getResourceAsStream("nihuaway00.properties")) {
            if (is != null) {
                Properties props = new Properties();
                props.load(is);
                return props;
            }
        } catch (IOException e) {
            if (log.isErrorEnabled()) {
                log.error("Failed to load nihuaway00.properties, using defaults", e);
            }
        }
        return new Properties();
    }
}
