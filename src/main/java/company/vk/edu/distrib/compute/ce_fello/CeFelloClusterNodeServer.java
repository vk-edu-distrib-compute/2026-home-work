package company.vk.edu.distrib.compute.ce_fello;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

final class CeFelloClusterNodeServer {
    private static final Logger log = LoggerFactory.getLogger(CeFelloClusterNodeServer.class);

    private final String localEndpoint;
    private final HttpServer server;
    private final Dao<byte[]> dao;
    private final CeFelloClusterProxyClient proxyClient;
    private final InetSocketAddress listenAddress;
    private final ExecutorService executor;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();

    CeFelloClusterNodeServer(String localEndpoint, CeFelloKeyDistributor distributor, Path storageDirectory)
            throws IOException {
        this.localEndpoint = Objects.requireNonNull(localEndpoint, "localEndpoint");
        this.listenAddress = newListenAddress(CeFelloClusterHttpHelper.endpointPort(localEndpoint));
        this.server = HttpServer.create();
        this.dao = new CeFelloFileSystemDao(storageDirectory);
        this.proxyClient = new CeFelloClusterProxyClient(localEndpoint);
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        server.setExecutor(executor);
        createContexts(Objects.requireNonNull(distributor, "distributor"));
    }

    void start() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("Node is already started");
        }

        try {
            server.bind(listenAddress, 0);
            server.start();
        } catch (IOException e) {
            started.set(false);
            executor.shutdownNow();
            closeDao();
            throw new UncheckedIOException("Failed to start HTTP server for " + localEndpoint, e);
        }
    }

    void stop() {
        if (!started.get() || !stopped.compareAndSet(false, true)) {
            return;
        }

        server.stop(0);
        executor.shutdownNow();
        closeDao();
    }

    private void createContexts(CeFelloKeyDistributor distributor) {
        server.createContext(
                CeFelloClusterHttpHelper.STATUS_PATH,
                wrapHandler(new CeFelloClusterStatusHandler())
        );
        server.createContext(
                CeFelloClusterHttpHelper.ENTITY_PATH,
                wrapHandler(new CeFelloClusterEntityHandler(localEndpoint, distributor, dao, proxyClient))
        );
    }

    private HttpHandler wrapHandler(HttpHandler handler) {
        return exchange -> {
            try (exchange) {
                try {
                    handler.handle(exchange);
                } catch (NoSuchElementException e) {
                    CeFelloClusterHttpHelper.sendEmpty(exchange, 404);
                } catch (IllegalArgumentException e) {
                    CeFelloClusterHttpHelper.sendEmpty(exchange, 400);
                } catch (Exception e) {
                    log.warn("Failed to handle request on {}", localEndpoint, e);
                    CeFelloClusterHttpHelper.sendEmpty(exchange, 503);
                }
            }
        };
    }

    @SuppressFBWarnings(
            value = "URLCONNECTION_SSRF_FD",
            justification = "The cluster nodes bind only to localhost for homework tests"
    )
    private static InetSocketAddress newListenAddress(int port) {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), port);
    }

    private void closeDao() {
        try {
            dao.close();
        } catch (IOException e) {
            log.warn("Failed to close DAO for {}", localEndpoint, e);
        }
    }
}
