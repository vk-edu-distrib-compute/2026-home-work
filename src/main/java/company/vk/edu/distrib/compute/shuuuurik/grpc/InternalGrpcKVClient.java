package company.vk.edu.distrib.compute.shuuuurik.grpc;

import com.google.protobuf.ByteString;
import company.vk.edu.distrib.compute.proto.shuuuurik.KeyRequest;
import company.vk.edu.distrib.compute.proto.shuuuurik.ReactorInternalKVServiceGrpc;
import company.vk.edu.distrib.compute.proto.shuuuurik.UpsertRequest;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

/**
 * gRPC-клиент для обращения к {@link InternalGrpcKVService} другой ноды кластера.
 * Один экземпляр клиента держит постоянный gRPC-канал к одной конкретной ноде.
 */
public class InternalGrpcKVClient {

    private static final Logger log = LoggerFactory.getLogger(InternalGrpcKVClient.class);

    private static final int SHUTDOWN_TIMEOUT_SECONDS = 5;
    private static final Duration CALL_TIMEOUT = Duration.ofSeconds(3);

    private final ManagedChannel channel;
    private final ReactorInternalKVServiceGrpc.ReactorInternalKVServiceStub stub;
    private final String grpcEndpoint;

    /**
     * Создаёт gRPC-клиент для обращения к {@link InternalGrpcKVService} другой ноды кластера.
     *
     * @param grpcEndpoint адрес gRPC-сервера целевой ноды, например "localhost:9090"
     */
    public InternalGrpcKVClient(String grpcEndpoint) {
        this.grpcEndpoint = grpcEndpoint;
        this.channel = Grpc.newChannelBuilder(grpcEndpoint, InsecureChannelCredentials.create()).build();
        this.stub = ReactorInternalKVServiceGrpc.newReactorStub(channel);
    }

    /**
     * Закрывает gRPC-канал. Вызывается при остановке ноды.
     */
    public void stop() {
        channel.shutdown();
        try {
            if (!channel.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.warn("gRPC channel to {} did not terminate in time, forcing shutdown", grpcEndpoint);
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            channel.shutdownNow();
        }
        log.info("gRPC client stopped, endpoint={}", grpcEndpoint);
    }

    /**
     * Отправляет GET на удалённую ноду.
     *
     * @param key ключ
     * @return значение или бросает {@link NoSuchElementException} если ключ не найден
     * @throws NoSuchElementException если ключ не найден на удалённой ноде
     */
    public byte[] get(String key) {
        KeyRequest request = KeyRequest.newBuilder().setKey(key).build();
        return stub.get(request)
                .timeout(CALL_TIMEOUT)
                .<byte[]>handle((response, sink) -> {
                    if (!response.hasValue()) {
                        sink.error(new NoSuchElementException("Key not found remotely: " + key));
                        return;
                    }
                    sink.next(response.getValue().getValue().toByteArray());
                })
                .block();
    }

    /**
     * Отправляет UPSERT на удалённую ноду.
     *
     * @param key   ключ
     * @param value значение
     */
    public void upsert(String key, byte[] value) {
        UpsertRequest request = UpsertRequest.newBuilder()
                .setKey(key)
                .setValue(ByteString.copyFrom(value))
                .build();
        stub.upsert(request)
                .timeout(CALL_TIMEOUT)
                .then(Mono.empty())
                .block();
    }

    /**
     * Отправляет DELETE на удалённую ноду.
     *
     * @param key ключ
     */
    public void delete(String key) {
        KeyRequest request = KeyRequest.newBuilder().setKey(key).build();
        stub.delete(request)
                .timeout(CALL_TIMEOUT)
                .then(Mono.empty())
                .block();
    }
}
