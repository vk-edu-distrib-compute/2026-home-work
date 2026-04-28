package company.vk.edu.distrib.compute.vitos23;

import company.vk.edu.distrib.compute.KVService;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/// Wraps several [KVService] instances into one.
/// [KVService#start()] and [KVService#stop()] delegate to all inner services.
public class ComposedKVService implements KVService {

    private final List<KVService> services;

    public ComposedKVService(List<KVService> services) {
        this.services = List.copyOf(services);
    }

    public static ComposedKVService of(KVService... services) {
        return new ComposedKVService(List.of(services));
    }

    @Override
    public void start() {
        for (KVService service : services) {
            service.start();
        }
    }

    @Override
    public void stop() {
        for (KVService service : services.reversed()) {
            service.stop();
        }
    }

    @Override
    public CompletableFuture<Void> awaitTermination() {
        CompletableFuture<?>[] futures = services.stream()
                .map(KVService::awaitTermination)
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures);
    }
}
