package company.vk.edu.distrib.compute.linempy.replication;

/**
 * Результат чтения: успех, данные, количество ответивших реплик.
 *
 * @author Linempy
 * @since 24.04.2026
 */
public record ReadResult(
        boolean success,
        byte[] value,
        int responded
) {
}
