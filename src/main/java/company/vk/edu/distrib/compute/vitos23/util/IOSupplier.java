package company.vk.edu.distrib.compute.vitos23.util;

import java.io.IOException;

@FunctionalInterface
public interface IOSupplier<T> {
    T get() throws IOException;
}
