package company.vk.edu.distrib.compute.nihuaway00.task5;

import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        final int nodeCount = 5;
        NodeRegistry registry = new NodeRegistry();
        registry.generateNodes(nodeCount);
        registry.runAll();
        Runtime.getRuntime().addShutdownHook(new Thread(registry::stopAll));

        try (Scanner scanner = new Scanner(System.in)) {
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
                int nodeId = Integer.parseInt(command);
                Node node = registry.getNode(nodeId);
                if (node == null) {
                    System.out.println("unknown node id: " + nodeId);
                    continue;
                }
                if (node.isAlive()) {
                    node.disable();
                } else {
                    node.enable();
                }
            }
        } finally {
            registry.stopAll();
        }
    }
}
