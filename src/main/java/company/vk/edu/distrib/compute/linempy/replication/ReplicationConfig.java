package company.vk.edu.distrib.compute.linempy.replication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Конфигурация репликации из переменных окружения.
 *
 * @author Linempy
 * @since 24.04.2026
 */
public class ReplicationConfig {
    private static final Logger log = LoggerFactory.getLogger(ReplicationConfig.class);

    private final int factor;
    private final int defaultAck;
    private final String storagePath;
    private final boolean asyncMode;

    public ReplicationConfig() {
        this.factor = Integer.parseInt(System.getenv().getOrDefault("REPLICATION_FACTOR", "3"));
        this.defaultAck = Integer.parseInt(System.getenv().getOrDefault("REPLICATION_ACK", "2"));
        this.storagePath = System.getenv().getOrDefault("REPLICATION_STORAGE_PATH", "./linempy_data");
        this.asyncMode = Boolean.parseBoolean(System.getenv().getOrDefault("REPLICATION_ASYNC", "false"));

        log.info("Replication config: factor={}, defaultAck={}, storagePath={}, asyncMode={}",
                factor, defaultAck, storagePath, asyncMode);
    }

    public int getFactor() {
        return factor;
    }

    public int getDefaultAck() {
        return defaultAck;
    }

    public String getStoragePath() {
        return storagePath;
    }

    public boolean isAsyncMode() {
        return asyncMode;
    }
}
