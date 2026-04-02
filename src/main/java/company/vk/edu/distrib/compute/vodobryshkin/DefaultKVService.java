package company.vk.edu.distrib.compute.vodobryshkin;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DefaultKVService implements KVService {
    private static final int STOP_DELAY = 1;
    private static final Logger log = LoggerFactory.getLogger("server");

    private final HttpServer httpServer;

    private boolean started;
    private boolean stopped;

    DefaultKVService(HttpServer httpServer) {
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
            throw new IllegalStateException("You Can't Start Http Server Twice");
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
            throw new IllegalStateException("You Can't Stop KVService Without Starting It");
        }
        if (stopped) {
            throw new IllegalStateException("You Can't Stop KVService Twice");
        }

        httpServer.stop(STOP_DELAY);
        stopped = true;

        log.debug("DefaultKVService stopped working");
    }
}
