package company.vk.edu.distrib.compute.linempy.task5;

/**
 * MessageType определяет логику обработки входящего сообщения в конечном автомате узла.
 * <ul>
 * <li>PING: проверка доступности текущего лидера.</li>
 * <li>ELECT: инициализация голосования, отправляемая узлам с большим ID.</li>
 * <li>ANSWER: подтверждение получения ELECT, останавливающее текущий узел от становления лидером.</li>
 * <li>VICTORY: уведомление всего кластера о выборе нового лидера.</li>
 * </ul>
 *
 * @author Linempy
 * @since 05.05.2026
 */
public enum MessageType {
    PING,
    ELECT,
    ANSWER,
    VICTORY
}
