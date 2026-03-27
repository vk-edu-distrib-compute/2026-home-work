package company.vk.edu.distrib.compute.vodobryshkin;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

class DefaultKVService implements KVService {
    private static final int STOP_DELAY = 1;
    private static final Logger log = LoggerFactory.getLogger("server");

    private final HttpServer httpServer;
    private final Dao<byte[]> storage;

    private boolean started = false;
    private boolean stopped = false;

    DefaultKVService(Dao<byte[]> storage, int port, int backlogSize) throws IOException {
        this(storage, HttpServer.create(new InetSocketAddress(port), backlogSize));
    }

    DefaultKVService(Dao<byte[]> storage, HttpServer httpServer) {
        this.storage = storage;
        this.httpServer = httpServer;
        log.debug("DefaultKVService was created");
    }

    /**
     * Bind storage to HTTP port and start listening.
     *
     * <p>
     * May be called only once.
     */
    @Override
    public void start() {
        if (started) {
            throw new RuntimeException("You Can't Start Http Server Twice");
        }

        httpServer.start();
        started = true;

        log.debug("DefaultKVService started working");
    }

    /**
     * Stop listening and free all the resources.
     *
     * <p>
     * May be called only once and after {@link #start()}.
     */
    @Override
    public void stop() {
        if (!started) {
            throw new RuntimeException("You Can't Stop KVService Without Starting It");
        }
        if (stopped) {
            throw new RuntimeException("You Can't Stop KVService Twice");
        }

        httpServer.stop(STOP_DELAY);
        stopped = true;

        log.debug("DefaultKVService stopped working");
    }
}
