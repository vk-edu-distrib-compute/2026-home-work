package company.vk.edu.distrib.compute.usl;

import java.nio.file.Path;

final class StoragePaths {
    private static final String ROOT_DIR_PROPERTY = "company.vk.edu.distrib.compute.usl.data.dir";
    private static final String DEFAULT_ROOT_DIR = "2026-home-work-usl";

    private StoragePaths() {
    }

    static Path persistentDataDir(int port) {
        String overrideRoot = System.getProperty(ROOT_DIR_PROPERTY);
        Path root = overrideRoot == null || overrideRoot.isBlank()
            ? Path.of(System.getProperty("java.io.tmpdir"), DEFAULT_ROOT_DIR)
            : Path.of(overrideRoot);
        return root.resolve("port-" + port);
    }
}
