package company.vk.edu.distrib.compute;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Constructs {@link KVService} instances.
 *
 */
public enum KVServiceFactory {
    ;
    private static final long MAX_HEAP = 128 * 1024 * 1024;

    /**
     * Construct a storage instance.
     *
     * @param port port to bind HTTP server to
     * @return a storage instance
     */
    public static KVService create(final int port) throws IOException {
        if (Runtime.getRuntime().maxMemory() > MAX_HEAP) {
            throw new IllegalStateException("The heap is too big. Consider setting Xmx.");
        }

        if (port <= 0 || 65536 <= port) {
            throw new IllegalArgumentException("Port out of range");
        }

        throw new UnsupportedEncodingException("not implemented yet");
    }
}
