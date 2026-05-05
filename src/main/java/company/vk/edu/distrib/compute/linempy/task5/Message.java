package company.vk.edu.distrib.compute.linempy.task5;

/**
 * Message — неизменяемая структура данных для межсерверного взаимодействия.
 *
 * @param type     Тип сообщения (PING, ELECT, ANSWER, VICTORY), определяющий этап алгоритма.
 * @param senderId Уникальный идентификатор узла-отправителя для приоритизации в Bully Algorithm.
 *
 * @author Linempy
 * @since 05.05.2026
 */
public record Message(
        MessageType type,
        int senderId
) {
}
