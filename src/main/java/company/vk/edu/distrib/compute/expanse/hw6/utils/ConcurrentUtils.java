package company.vk.edu.distrib.compute.expanse.hw6.utils;

import java.util.concurrent.atomic.AtomicLong;

public final class ConcurrentUtils {
    private static final AtomicLong SEQUENCE = new AtomicLong(0);

    private ConcurrentUtils() {
    }


    public static long getNextId() {
        return SEQUENCE.getAndIncrement();
    }
}
