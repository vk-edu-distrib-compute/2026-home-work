package company.vk.edu.distrib.compute.tadzhnahal.consensus;

import java.lang.System.Logger;
import java.util.function.Supplier;

final class LogHelper {
    private LogHelper() {
    }

    static void info(Logger logger, String message) {
        if (logger.isLoggable(Logger.Level.INFO)) {
            logger.log(Logger.Level.INFO, message);
        }
    }

    static void info(Logger logger, Supplier<String> messageSupplier) {
        if (logger.isLoggable(Logger.Level.INFO)) {
            logger.log(Logger.Level.INFO, messageSupplier.get());
        }
    }
}
