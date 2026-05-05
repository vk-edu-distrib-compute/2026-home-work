package company.vk.edu.distrib.compute.martinez1337.consensus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ClusterRunner {
    static void main() throws InterruptedException {
        int N = 5;
        List<Node> nodes = new ArrayList<>();
        for (int i = 1; i <= N; i++) {
            nodes.add(new Node(i, nodes));
        }
        nodes.forEach(Thread::start);

        ScheduledExecutorService printer = Executors.newScheduledThreadPool(1);
        printer.scheduleAtFixedRate(() -> {
            System.out.println("=== Cluster State ===");
            nodes.forEach(n -> System.out.printf("Node %d: alive=%b, leader=%d%n",
                    n.getNodeId(), n.isUp(), n.getLeaderId()));
        }, 2, 1, TimeUnit.SECONDS);

        // Пример внешнего воздействия: убить узел 5 через 10 сек
        Thread.sleep(10_000);
        nodes.get(4).crash();
    }
}
