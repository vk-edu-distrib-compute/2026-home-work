package company.vk.edu.distrib.compute.expanse.utils;

import company.vk.edu.distrib.compute.expanse.exception.InternalException;

import java.util.Arrays;

public final class ExceptionUtils {
    private ExceptionUtils() {
    }

    public static InternalException wrapToInternal(Exception e) {
        String message = String.format("Received internal exception of type %s with message %s and stacktrace:%n%s,",
                e.getClass().getSimpleName(), e.getMessage(), Arrays.toString(e.getStackTrace()));
        return new InternalException(message);
    }
}
