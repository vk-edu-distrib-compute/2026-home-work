package company.vk.edu.distrib.compute.shuuuurik.grpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Empty;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.proto.shuuuurik.GetResponse;
import company.vk.edu.distrib.compute.proto.shuuuurik.KeyRequest;
import company.vk.edu.distrib.compute.proto.shuuuurik.ReactorInternalKVServiceGrpc;
import company.vk.edu.distrib.compute.proto.shuuuurik.UpsertRequest;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

/**
 * gRPC-сервер, принимающий внутренние запросы от других нод кластера.
 * Делегирует операции в локальный {@link Dao}.
 * Запускается рядом с HTTP-сервером на отдельном порту.
 *
 * <p>Поддерживает повторный запуск: каждый вызов {@link #start()} пересоздаёт
 * внутренний {@link Server}, так как gRPC Server является одноразовым объектом.
 */
public class InternalGrpcKVService extends ReactorInternalKVServiceGrpc.InternalKVServiceImplBase implements KVService {

    private static final Logger log = LoggerFactory.getLogger(InternalGrpcKVService.class);

    private final int grpcPort;
    private final Dao<byte[]> dao;

    /**
     * Текущий экземпляр gRPC-сервера. Пересоздаётся при каждом {@link #start()}.
     */
    private Server grpcServer;

    /**
     * Создаёт gRPC-сервер, принимающий внутренние запросы от других нод кластера.
     *
     * @param grpcPort порт gRPC-сервера (отдельный от HTTP)
     * @param dao      локальное хранилище данного узла
     */
    public InternalGrpcKVService(int grpcPort, Dao<byte[]> dao) {
        super();
        this.grpcPort = grpcPort;
        this.dao = dao;
    }

    /**
     * Запускает gRPC-сервер. Пересоздаёт внутренний Server при каждом вызове.
     */
    @Override
    public void start() {
        try {
            grpcServer = Grpc.newServerBuilderForPort(grpcPort, InsecureServerCredentials.create())
                    .addService(this)
                    .build();
            grpcServer.start();
            if (log.isInfoEnabled()) {
                log.info("gRPC server started on port {}", grpcServer.getPort());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to start gRPC server", e);
        }
    }

    /**
     * Останавливает gRPC-сервер и ждёт завершения всех активных RPC.
     */
    @Override
    public void stop() {
        if (grpcServer == null) {
            return;
        }
        grpcServer.shutdown();
        try {
            if (!grpcServer.awaitTermination(5, TimeUnit.SECONDS)) {
                grpcServer.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while waiting for gRPC server termination");
        }
        log.info("gRPC server stopped");
    }

    /**
     * Обрабатывает входящий GET от другой ноды.
     * Если ключ не найден - возвращает GetResponse без value (hasValue() == false).
     */
    @Override
    public Mono<GetResponse> get(KeyRequest request) {
        try {
            byte[] value = dao.get(request.getKey());
            GetResponse response = GetResponse.newBuilder()
                    .setValue(BytesValue.of(ByteString.copyFrom(value)))
                    .build();
            return Mono.just(response);
        } catch (NoSuchElementException e) {
            // Ключ не найден - возвращается пустой GetResponse (value отсутствует)
            return Mono.just(GetResponse.newBuilder().build());
        } catch (IOException e) {
            if (log.isErrorEnabled()) {
                log.error("gRPC Get failed for key={}", request.getKey(), e);
            }
            return Mono.error(e);
        }
    }

    /**
     * Обрабатывает входящий UPSERT от другой ноды.
     */
    @Override
    public Mono<Empty> upsert(UpsertRequest request) {
        try {
            dao.upsert(request.getKey(), request.getValue().toByteArray());
            return Mono.just(Empty.getDefaultInstance());
        } catch (IOException e) {
            if (log.isErrorEnabled()) {
                log.error("gRPC Upsert failed for key={}", request.getKey(), e);
            }
            return Mono.error(e);
        }
    }

    /**
     * Обрабатывает входящий DELETE от другой ноды.
     */
    @Override
    public Mono<Empty> delete(KeyRequest request) {
        try {
            dao.delete(request.getKey());
            return Mono.just(Empty.getDefaultInstance());
        } catch (IOException e) {
            if (log.isErrorEnabled()) {
                log.error("gRPC Delete failed for key={}", request.getKey(), e);
            }
            return Mono.error(e);
        }
    }
}
