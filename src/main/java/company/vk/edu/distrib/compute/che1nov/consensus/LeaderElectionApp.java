package company.vk.edu.distrib.compute.che1nov.consensus;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.List;

public final class LeaderElectionApp {
    private static final Logger LOGGER = System.getLogger(LeaderElectionApp.class.getName());

    private LeaderElectionApp() {
    }

    public static void main(String[] args) {
        ClusterModel cluster = new ClusterModel(
                List.of(1, 2, 3, 4, 5),
                Duration.ofMillis(300),
                Duration.ofMillis(600),
                Duration.ofMillis(700),
                0.0,
                Duration.ofMillis(500),
                Duration.ofMillis(1_500)
        );

        cluster.start();
        try {
            for (int i = 0; i < 20; i++) {
                sleep(300);
                LOGGER.log(Level.INFO, cluster.snapshot());
            }
        } finally {
            cluster.stop();
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
