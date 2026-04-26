package company.vk.edu.distrib.compute.expanse.context;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.expanse.handler.HandlerWrapper;
import company.vk.edu.distrib.compute.expanse.model.ApiSettings;

import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class WebContextUtils {
    private static final Logger log = Logger.getLogger(WebContextUtils.class.getName());

    private WebContextUtils() {
    }

    public static void init(HttpServer httpServer) {
        Objects.requireNonNull(httpServer);

        for (ApiSettings settings : ApiSettings.values()) {
            if (log.isLoggable(Level.INFO)) {
                log.info("Initializing handler for path: " + settings.getPath());
            }
            HttpHandler handler = (HttpHandler) AppContextUtils.getBean(settings.getHandlerClass());
            httpServer.createContext(settings.getPath(), new HandlerWrapper(settings, handler));
        }
    }
}
