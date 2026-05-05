package company.vk.edu.distrib.compute.nihuaway00.task5;

import java.util.Scanner;

public class Main {
    private static final double AUTO_FAILURE_PROBABILITY = 0.15;
    private static final long DICE_INTERVAL_MS = 3_000;
    private static final long MIN_RECOVERY_MS = 2_000;
    private static final long MAX_RECOVERY_MS = 5_000;

    public static void main(String[] args) {
        final int nodeCount = 5;
        NodeRegistry registry = new NodeRegistry();
        registry.generateNodes(
                nodeCount,
                NodeConfig.defaultConfig(),
                new FailureConfig(
                        AUTO_FAILURE_PROBABILITY,
                        DICE_INTERVAL_MS,
                        MIN_RECOVERY_MS,
                        MAX_RECOVERY_MS
                )
        );
        registry.runAll();
        Runtime.getRuntime().addShutdownHook(new Thread(registry::stopAll));

        try (Scanner scanner = new Scanner(System.in)) {
            printHelp();
            while (scanner.hasNextLine()) {
                String command = scanner.nextLine().trim();
                if (command.isEmpty()) {
                    continue;
                }
                if ("exit".equalsIgnoreCase(command)) {
                    break;
                }
                if ("state".equalsIgnoreCase(command)) {
                    registry.getAllNodes().forEach(node ->
                            System.out.println(
                                    "node=" + node.getId()
                                            + " alive=" + node.isAlive()
                                            + " state=" + node.getState()
                                            + " leader=" + node.getCurrentLeaderId()
                            )
                    );
                    continue;
                }
                if ("help".equalsIgnoreCase(command)) {
                    printHelp();
                    continue;
                }

                String[] parts = command.split("\\s+");
                if (parts.length == 2 && "down".equalsIgnoreCase(parts[0])) {
                    setNodeEnabled(registry, parts[1], false);
                    continue;
                }
                if (parts.length == 2 && "up".equalsIgnoreCase(parts[0])) {
                    setNodeEnabled(registry, parts[1], true);
                    continue;
                }
                if (parts.length == 1) {
                    toggleNode(registry, parts[0]);
                    continue;
                }
                System.out.println("unknown command: " + command);
            }
        } finally {
            registry.stopAll();
        }
    }

    private static void toggleNode(NodeRegistry registry, String nodeIdRaw) {
        Integer nodeId = parseNodeId(nodeIdRaw);
        if (nodeId == null) {
            return;
        }
        Node node = registry.getNode(nodeId);
        if (node == null) {
            System.out.println("unknown node id: " + nodeId);
            return;
        }
        if (node.isAlive()) {
            node.disable();
        } else {
            node.enable();
        }
    }

    private static void setNodeEnabled(NodeRegistry registry, String nodeIdRaw, boolean enabled) {
        Integer nodeId = parseNodeId(nodeIdRaw);
        if (nodeId == null) {
            return;
        }
        Node node = registry.getNode(nodeId);
        if (node == null) {
            System.out.println("unknown node id: " + nodeId);
            return;
        }
        if (enabled) {
            node.enable();
        } else {
            node.disable();
        }
    }

    private static Integer parseNodeId(String nodeIdRaw) {
        try {
            return Integer.parseInt(nodeIdRaw);
        } catch (NumberFormatException expected) {
            System.out.println("invalid node id: " + nodeIdRaw);
            return null;
        }
    }

    private static void printHelp() {
        System.out.println("Commands:");
        System.out.println("  state      - show all nodes");
        System.out.println("  down <id>  - force node DOWN");
        System.out.println("  up <id>    - force node UP");
        System.out.println("  <id>       - toggle node state");
        System.out.println("  help       - show this help");
        System.out.println("  exit       - stop the program");
    }
}
