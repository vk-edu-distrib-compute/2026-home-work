package company.vk.edu.distrib.compute.bushuev_a_s;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.bushuev_a_s.MyFileDao.DaoException;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.Executors;

public class MyKVService implements KVService {
    private static final Logger log = LoggerFactory.getLogger(MyKVService.class);
    private static final String ID_PREFIX = "id=";

    private final MyKVCluster cluster;
    private final String myEndpoint;
    private static final HttpClient httpClient = HttpClient.newBuilder()
            .executor(Executors.newVirtualThreadPerTaskExecutor())
            .build();

    private final HttpServer server;
    private final Dao<byte[]> dao;

    /**
     * Создаёт новый экземпляр сервиса хранения ключ-значение.
     * @param port порт для запуска HTTP сервера
     * @param dao  DAO для работы с хранилищем данных
     * @throws IOException если не удаётся создать HTTP сервер
     */
    public MyKVService(int port, Dao<byte[]> dao, MyKVCluster cluster) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = dao;
        this.cluster = cluster;
        this.myEndpoint = "http://localhost:" + port;

        server.createContext("/v0/status", new ErrorHttpHandler(
                this::handleStatus));

        server.createContext("/v0/entity", new ErrorHttpHandler(
                this::handleEntity
        ));
    }

    public MyKVService(int port, Dao<byte[]> dao) throws IOException {
        this(port, dao, null);
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

        String targetEndpoint = cluster.getEndpoint(id);

        if (!myEndpoint.equals(targetEndpoint)) {
            proxyRequest(http, targetEndpoint);
            return;
        }

        switch (method) {
            case "GET":
                final byte[] value = dao.get(id);
                try (var os = http.getResponseBody()) {
                    http.sendResponseHeaders(200, value.length);
                    os.write(value);
                }
                break;
            case "PUT":
                try (InputStream is = http.getRequestBody()) {
                    final byte[] upsertValue = is.readAllBytes();
                    dao.upsert(id, upsertValue);
                }
                http.sendResponseHeaders(201, 0);
                http.getResponseBody().close();
                break;
            case "DELETE":
                dao.delete(id);
                http.sendResponseHeaders(202, 0);
                http.getResponseBody().close();
                break;
            default:
                http.sendResponseHeaders(405, 0);
                http.getResponseBody().close();
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

    private void proxyRequest(HttpExchange clientExchange, String targetEndpoint) throws IOException {
        URI targetUri = URI.create(targetEndpoint + clientExchange.getRequestURI().getPath()
                + "?" + clientExchange.getRequestURI().getQuery());

        HttpRequest.BodyPublisher bodyPublisher = "PUT".equals(clientExchange.getRequestMethod())
                ? HttpRequest.BodyPublishers.ofByteArray(clientExchange.getRequestBody().readAllBytes())
                : HttpRequest.BodyPublishers.noBody();

        HttpRequest proxyRequest = HttpRequest.newBuilder()
                .uri(targetUri)
                .method(clientExchange.getRequestMethod(), bodyPublisher)
                .build();

        try {
            HttpResponse<byte[]> response = httpClient.send(proxyRequest, HttpResponse.BodyHandlers.ofByteArray());

            clientExchange.sendResponseHeaders(response.statusCode(),
                    response.body().length == 0 ? -1 : response.body().length);
            if (response.body().length > 0) {
                try (var os = clientExchange.getResponseBody()) {
                    os.write(response.body());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            clientExchange.sendResponseHeaders(500, -1);
        }
    }

    @Override
    public void start() {
        log.info("Starting");
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
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
         *
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
                exchange.sendResponseHeaders(400, -1);
            } catch (NoSuchElementException e) {
                log.debug("Not found", e); // Добавлено логирование
                exchange.sendResponseHeaders(404, -1);
            } catch (IOException e) {
                log.error("Internal server error", e); // Добавлено логирование
                exchange.sendResponseHeaders(500, -1);
            } catch (DaoException e) {
                log.error("Storage error", e);
                exchange.sendResponseHeaders(500, -1);
            } catch (MyKVCluster.StartException e) {
                log.error("Failed to start server", e);
                exchange.sendResponseHeaders(500, -1);
            } catch (MyKVCluster.NoAlgorithmException e) {
                log.error("Hash algorithm not found", e);
                exchange.sendResponseHeaders(500, -1);
            }
        }
    }
}
