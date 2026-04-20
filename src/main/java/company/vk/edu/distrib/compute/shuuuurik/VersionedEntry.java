package company.vk.edu.distrib.compute.shuuuurik;

import java.io.Serial;
import java.io.Serializable;

/**
 * Версионированная запись для хранилища реплик.
 * Содержит само значение, timestamp записи и флаг tombstone (признак удаления).
 */
public class VersionedEntry implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Значение; null если tombstone.
     */
    private final byte[] value;

    /**
     * Timestamp записи (System.currentTimeMillis()).
     */
    private final long timestamp;

    /**
     * true если эта запись является маркером удаления.
     */
    private final boolean tombstone;

    /**
     * Создаёт живую запись с данными.
     *
     * @param value     значение (не null)
     * @param timestamp время записи
     */
    VersionedEntry(byte[] value, long timestamp) {
        this.value = value == null ? null : value.clone();
        this.timestamp = timestamp;
        this.tombstone = false;
    }

    /**
     * Создаёт tombstone - маркер удаления.
     *
     * @param timestamp время удаления
     */
    VersionedEntry(long timestamp) {
        this.value = null;
        this.timestamp = timestamp;
        this.tombstone = true;
    }

    byte[] getValue() {
        return value == null ? null : value.clone();
    }

    long getTimestamp() {
        return timestamp;
    }

    boolean isTombstone() {
        return tombstone;
    }
}
