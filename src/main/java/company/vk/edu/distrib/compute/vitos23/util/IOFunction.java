package company.vk.edu.distrib.compute.vitos23.util;

import java.io.IOException;

@FunctionalInterface
public interface IOFunction<T, R> {
    R apply(T input) throws IOException, InterruptedException;
}
