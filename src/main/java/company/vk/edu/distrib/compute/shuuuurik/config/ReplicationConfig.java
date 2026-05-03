package company.vk.edu.distrib.compute.shuuuurik.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ReplicationConfig {

    /**
     * Максимальное число узлов в кластере.
     * replication.factor не может превышать это значение.
     */
    public static final int MAX_CLUSTER_SIZE = 10;
    private static final Logger log = LoggerFactory.getLogger(ReplicationConfig.class);
    private static final String PROPERTIES_PATH = "/company/vk/edu/distrib/compute/shuuuurik/replication.properties";
    private static final String KEY_REPLICATION_FACTOR = "replication.factor";
    private static final int DEFAULT_REPLICATION_FACTOR = 1;
    private final int replicationFactor;

    /**
     * Загружает конфигурацию из classpath-ресурса.
     *
     * <p>Если файл не найден или свойство отсутствует -
     * использует дефолтное значение ({@value DEFAULT_REPLICATION_FACTOR}).
     * Если свойство присутствует, но содержит невалидное значение -
     * выбрасывает {@link IllegalArgumentException}.
     *
     * @throws IllegalArgumentException если значение replication.factor не является
     *                                  числом или выходит за допустимые границы
     */
    public ReplicationConfig() {
        this.replicationFactor = loadReplicationFactor();
    }

    /**
     * Загружает значение {@code replication.factor} из properties-файла в classpath.
     *
     * <p>Если файл не найден или свойство отсутствует - возвращает {@value DEFAULT_REPLICATION_FACTOR}.
     * Если свойство присутствует, но невалидно (не число, <= 0 или > MAX_CLUSTER_SIZE) -
     * выбрасывает {@link IllegalArgumentException}.
     *
     * @return фактор репликации
     * @throws IllegalArgumentException если значение свойства невалидно
     */
    private static int loadReplicationFactor() {
        Properties props = new Properties();

        try (InputStream in = ReplicationConfig.class.getResourceAsStream(PROPERTIES_PATH)) {
            if (in == null) {
                log.warn("replication.properties not found at {}, using default n={}",
                        PROPERTIES_PATH, DEFAULT_REPLICATION_FACTOR);
                return DEFAULT_REPLICATION_FACTOR;
            }
            props.load(in);
        } catch (IOException e) {
            log.warn("Failed to load replication.properties, using default n={}", DEFAULT_REPLICATION_FACTOR, e);
            return DEFAULT_REPLICATION_FACTOR;
        }

        String value = props.getProperty(KEY_REPLICATION_FACTOR);
        if (value == null || value.isBlank()) {
            log.info("replication.factor not set in properties, using default n={}", DEFAULT_REPLICATION_FACTOR);
            return DEFAULT_REPLICATION_FACTOR;
        }

        try {
            int parsed = Integer.parseInt(value.trim());
            if (parsed <= 0) {
                throw new IllegalArgumentException(KEY_REPLICATION_FACTOR + " must be > 0, got: " + parsed);
            }
            if (parsed > MAX_CLUSTER_SIZE) {
                throw new IllegalArgumentException(
                        KEY_REPLICATION_FACTOR + " must be <= " + MAX_CLUSTER_SIZE + ", got: " + parsed);
            }
            log.info("Loaded replication.factor = {}", parsed);
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid value for " + KEY_REPLICATION_FACTOR + ": " + value, e);
        }
    }

    /**
     * Возвращает фактор репликации n - на скольких репликах хранится каждый ключ.
     */
    public int getReplicationFactor() {
        return replicationFactor;
    }
}
