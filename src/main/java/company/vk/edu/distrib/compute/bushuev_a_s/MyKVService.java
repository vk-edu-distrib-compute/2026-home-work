package company.vk.edu.distrib.compute.bushuev_a_s;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;
import java.util.Objects;

public class MyKVService implements KVService {
    private static final Logger log = LoggerFactory.getLogger(MyKVService.class);
    private static final String ID_PREFIX = "id=";

    private final HttpServer server;
    private final Dao<byte[]> dao;

    /**
     * Создаёт новый экземпляр сервиса хранения ключ-значение.
     * @param port порт для запуска HTTP сервера
     * @param dao  DAO для работы с хранилищем данных
     * @throws IOException если не удаётся создать HTTP сервер
     */
    public MyKVService(int port, Dao<byte[]> dao) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = dao;

        server.createContext("/v0/status", new ErrorHttpHandler(
                this::handleStatus));

        server.createContext("/v0/entity", new ErrorHttpHandler(
                this::handleEntity
        ));
    }

    private void handleStatus(HttpExchange http) throws IOException {
        final var method = http.getRequestMethod();
        if (Objects.equals("GET",method)) {
            http.sendResponseHeaders(200, -1);
            // No response body required for status check
        } else {
            http.sendResponseHeaders(405, -1);
        }

    }

    private void handleEntity(HttpExchange http) throws IOException {
        final var method = http.getRequestMethod();
        final var query = http.getRequestURI().getQuery();
        final String id = parceId(query);

        // No response body required for status check
        switch (method) {
            case "GET":
                // No response body required for status check
                final byte[] value = dao.get(id);
                try (var os = http.getResponseBody()) {
                    http.sendResponseHeaders(200, value.length);
                    os.write(value); // Запись данных в поток
                }
                break;
            case "PUT":
                // No response body required for status check
                try (InputStream is = http.getRequestBody()) {
                    // No response body required for status check
                    final byte[] upsertValue = is.readAllBytes();
                    dao.upsert(id, upsertValue);
                }
                http.sendResponseHeaders(201, -1);
                break;
            case "DELETE":
                // No response body required for status check
                dao.delete(id);
                http.sendResponseHeaders(202, -1);
                break;
            default:
                // No response body required for status check
                http.sendResponseHeaders(405, -1);
                break;
        }
    }

    /**
     * Парсит ID из строки запроса.
     *
     * @param query строка запроса в формате "id=значение"
     * @return значение ID
     * @throws IllegalArgumentException если формат запроса некорректен
     */
    private static String parceId(String query) {
        if (query != null && query.startsWith(ID_PREFIX)) {
            return query.substring(ID_PREFIX.length());
        } else {
            throw new IllegalArgumentException("bad query");
        }
    }

    @Override
    public void start() {
        log.info("Starting");
        server.start();
    }

    @Override
    public void stop() {
        server.stop(1);
        log.info("Stopped");
    }

    /**
     * Обработчик HTTP запросов с обработкой ошибок.
     */
    private static final class ErrorHttpHandler implements HttpHandler {
        private final HttpHandler delegate;

        /**
         * Создаёт обёртку для обработчика с перехватом исключений.
         * @param delegate делегируемый обработчик
         */
        private ErrorHttpHandler(HttpHandler delegate) {
            this.delegate = delegate;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try (exchange) {
                // No response body required for status check
                delegate.handle(exchange);
            } catch (IllegalArgumentException e) {
                log.debug("Bad request", e); // Добавлено логирование
                exchange.sendResponseHeaders(400, 0);
            } catch (NoSuchElementException e) {
                log.debug("Not found", e); // Добавлено логирование
                exchange.sendResponseHeaders(404, 0);
            } catch (IOException e) {
                log.error("Internal server error", e); // Добавлено логирование
                exchange.sendResponseHeaders(500, 0);
            }
        }
    }
}
