package company.vk.edu.distrib.compute.martinez1337.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class ClusterHelper {
    private static final Logger log = LoggerFactory.getLogger(ClusterHelper.class);

    private ClusterHelper() {
    }

    static void main() {
        int n = 5;
        List<Node> nodes = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            nodes.add(new Node(i, nodes));
        }
        nodes.forEach(Thread::start);

        ScheduledExecutorService printer = Executors.newScheduledThreadPool(1);

        printer.scheduleAtFixedRate(() -> {
            try {
                StringBuilder state = new StringBuilder("\n=== Cluster State ===\n");
                for (Node node : nodes) {
                    state.append(String.format("Node %d: alive=%b, leader=%d\n",
                            node.getNodeId(), node.isUp(), node.getLeaderId()));
                }
                log.info(state.toString());
            } catch (Exception e) {
                log.error("Error while printing state", e);
            }
        }, 2, 1, TimeUnit.SECONDS);

        try (Scanner scanner = new Scanner(System.in)) {
            log.info("Simulation started. Press ENTER to stop...");

            Thread.sleep(10_000);
            if (nodes.size() >= n) {
                nodes.get(4).crash();
                log.warn("Node 5 has been crashed!");
            }

            if (scanner.hasNextLine()) {
                scanner.nextLine();
            }
        } catch (InterruptedException e) {
            log.error("Main thread interrupted", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Unexpected error", e);
        } finally {
            printer.shutdownNow();
            log.info("Simulation finished.");
        }
    }
}
