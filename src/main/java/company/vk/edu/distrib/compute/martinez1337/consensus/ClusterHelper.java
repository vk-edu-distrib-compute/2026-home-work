package company.vk.edu.distrib.compute.martinez1337.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class ClusterHelper {
    private static final Logger log = LoggerFactory.getLogger(ClusterHelper.class);

    private ClusterHelper() {
    }

    static void main() throws InterruptedException {
        int n = 5;
        List<Node> nodes = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            nodes.add(new Node(i, nodes));
        }
        nodes.forEach(Thread::start);

        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out));
        ScheduledExecutorService printer = Executors.newScheduledThreadPool(1);
        printer.scheduleAtFixedRate(() -> {
            try {
                bw.write("=== Cluster State ===");
                bw.newLine();

                for (Node node : nodes) {
                    bw.write(String.format("Node %d: alive=%b, leader=%d",
                            node.getNodeId(), node.isUp(), node.getLeaderId()));
                    bw.newLine();
                }
                bw.flush();
            } catch (IOException e) {
                log.warn(e.getMessage());
            }
        }, 2, 1, TimeUnit.SECONDS);

        // Пример внешнего воздействия: убить узел 5 через 10 сек
        Thread.sleep(10_000);
        nodes.get(4).crash();
    }
}
