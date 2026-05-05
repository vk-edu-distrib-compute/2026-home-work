package company.vk.edu.distrib.compute.andeco.election;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class LeaderElectionApp {
    private LeaderElectionApp() {
    }

    public static void main(String[] args) throws InterruptedException {
        int n = intArg(args, "--nodes", 5);
        long pingTimeoutMs = longArg(args, "--ping-timeout-ms", 1500);
        long answerTimeoutMs = longArg(args, "--answer-timeout-ms", 600);
        long victoryTimeoutMs = longArg(args, "--victory-timeout-ms", 1200);
        double failProb = doubleArg(args, "--fail-prob", 0.0);
        long minRecoverMs = longArg(args, "--min-recover-ms", 800);
        long maxRecoverMs = longArg(args, "--max-recover-ms", 2500);
        long seed = longArg(args, "--seed", 1);

        List<Integer> ids = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            ids.add(i);
        }

        List<ElectionNode> nodes = new ArrayList<>();
        Cluster cluster = new Cluster(nodes);
        ElectionConfig config = ElectionConfig.defaults(pingTimeoutMs, answerTimeoutMs, victoryTimeoutMs);

        for (int id : ids) {
            nodes.add(new ElectionNode(id, ids, config, pingTimeoutMs, answerTimeoutMs, victoryTimeoutMs,
                    failProb, minRecoverMs, maxRecoverMs, seed));
        }
        for (ElectionNode node : nodes) {
            node.attachCluster(cluster);
        }
        nodes.forEach(Thread::start);
        final long monitorPeriodMs = longArg(args, "--monitor-period-ms", 700);
        ClusterMonitor monitor = null;
        if (monitorPeriodMs > 0) {
            monitor = new ClusterMonitor(cluster, monitorPeriodMs);
            monitor.start();
        }
        long runSeconds = longArg(args, "--run-seconds", 5);
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(runSeconds);
        while (System.nanoTime() < until) {
            TimeUnit.MILLISECONDS.sleep(500);
        }
        close(nodes, monitor);
    }

    private static void close(List<ElectionNode> nodes, ClusterMonitor monitor) throws InterruptedException {
        for (ElectionNode node : nodes) {
            node.shutdown();
        }
        for (ElectionNode node : nodes) {
            node.join(2000);
        }
        if (monitor != null) {
            monitor.shutdown();
        }
    }

    private static int intArg(String[] args, String key, int def) {
        String v = arg(args, key);
        if (v == null) {
            return def;
        }
        return Integer.parseInt(v);
    }

    private static long longArg(String[] args, String key, long def) {
        String v = arg(args, key);
        if (v == null) {
            return def;
        }
        return Long.parseLong(v);
    }

    private static double doubleArg(String[] args, String key, double def) {
        String v = arg(args, key);
        if (v == null) {
            return def;
        }
        return Double.parseDouble(v);
    }

    private static String arg(String[] args, String key) {
        for (int i = 0; i < args.length - 1; i++) {
            if (key.equals(args[i])) {
                return args[i + 1];
            }
        }
        return null;
    }
}

