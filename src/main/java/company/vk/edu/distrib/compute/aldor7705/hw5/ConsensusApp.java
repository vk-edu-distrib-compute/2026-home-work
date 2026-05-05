package company.vk.edu.distrib.compute.aldor7705.hw5;

import java.util.concurrent.TimeUnit;

public class ConsensusApp {
    private static final int CLUSTER_SIZE = 5;

    public static void main(String[] args) throws InterruptedException {
        System.out.println("=".repeat(60));
        System.out.println("РАСПРЕДЕЛЁННЫЙ КОНСЕНСУС - АЛГОРИТМ BULLY");
        System.out.println("=".repeat(60));

        scenario1CleanStart();
        scenario2LeaderFailure();
        scenario3NodeRecovery();
        scenario4RapidFailures();

        System.out.println("\nВсе сценарии завершены!");
    }

    private static void scenario1CleanStart() throws InterruptedException {
        printHeader(1, "НОРМАЛЬНЫЙ СТАРТ - выборы при запуске кластера");

        Cluster cluster = new Cluster(CLUSTER_SIZE);
        cluster.start();
        Thread.sleep(500);

        cluster.startInitialElection();
        sleep(5000);

        cluster.printStatus();
        assertLeader(cluster, CLUSTER_SIZE);

        cluster.shutdown();
        printFooter(1);
    }

    private static void scenario2LeaderFailure() throws InterruptedException {
        printHeader(2, "ОТКАЗ ЛИДЕРА - должен выбраться новый лидер");

        Cluster cluster = new Cluster(CLUSTER_SIZE);
        cluster.start();
        Thread.sleep(500);
        cluster.startInitialElection();
        sleep(5000);

        cluster.printStatus();
        int originalLeader = cluster.getConsensusLeader();
        System.out.println("Первоначальный лидер: " + originalLeader);

        cluster.getNode(originalLeader).forceDown();
        System.out.println("\n>>> Узел " + originalLeader + " (ЛИДЕР) принудительно отключён <<<\n");

        sleep(8000);
        cluster.printStatus();

        assertLeader(cluster, originalLeader - 1);

        cluster.shutdown();
        printFooter(2);
    }

    private static void scenario3NodeRecovery() throws InterruptedException {
        printHeader(3, "ВОССТАНОВЛЕНИЕ УЗЛА - не должно нарушить консенсус");

        Cluster cluster = new Cluster(CLUSTER_SIZE);
        cluster.start();
        Thread.sleep(500);
        cluster.startInitialElection();
        sleep(5000);

        cluster.printStatus();
        int originalLeader = cluster.getConsensusLeader();

        int failingNode = 2;
        if (failingNode == originalLeader) {
            failingNode = 1;
        }

        cluster.getNode(failingNode).forceDown();
        System.out.println("\n>>> Узел " + failingNode + " принудительно отключён <<<\n");

        sleep(3000);

        cluster.getNode(failingNode).forceRecover();
        System.out.println("\n>>> Узел " + failingNode + " принудительно восстановлен <<<\n");

        sleep(5000);
        cluster.printStatus();

        assertLeader(cluster, originalLeader);

        cluster.shutdown();
        printFooter(3);
    }

    private static void scenario4RapidFailures() throws InterruptedException {
        printHeader(4, "ЧАСТЫЕ ОТКАЗЫ/ВОССТАНОВЛЕНИЯ - проверка стабильности");

        Cluster cluster = new Cluster(CLUSTER_SIZE);
        cluster.start();
        Thread.sleep(500);
        cluster.startInitialElection();
        sleep(5000);

        System.out.println("Начинаю цикл частых отказов и восстановлений...\n");

        for (int round = 1; round <= 5; round++) {
            int nodeId = (round % (CLUSTER_SIZE - 1)) + 1;

            System.out.printf("Раунд %d: Отключаю узел %d...%n", round, nodeId);
            cluster.getNode(nodeId).forceDown();
            sleep(1000);

            System.out.printf("Раунд %d: Восстанавливаю узел %d...%n", round, nodeId);
            cluster.getNode(nodeId).forceRecover();
            sleep(2000);

            cluster.printStatus();

            int leader = cluster.getConsensusLeader();
            if (leader == -2) {
                System.out.println("[ПРОВАЛ] Обнаружен split-brain!");
            } else if (leader < 0) {
                System.out.println("[ПРЕДУПРЕЖДЕНИЕ] Лидер отсутствует - выборы ещё идут");
            } else {
                System.out.println("[OK] Лидер: " + leader);
            }
        }

        sleep(5000);
        cluster.printStatus();

        int finalLeader = cluster.getConsensusLeader();
        if (finalLeader == -2) {
            System.out.println("[ПРОВАЛ] Split-brain в финальном состоянии!");
        } else if (finalLeader > 0) {
            System.out.println("[OK] Итоговый лидер: " + finalLeader);
        }

        cluster.shutdown();
        printFooter(4);
    }

    private static void assertLeader(Cluster cluster, int expectedLeader) {
        int actual = cluster.getConsensusLeader();
        if (actual == -2) {
            System.out.println("[ПРОВАЛ] Обнаружен split-brain!");
        } else if (actual < 0) {
            System.out.println("[ПРЕДУПРЕЖДЕНИЕ] Консенсус не достигнут");
        } else if (expectedLeader > 0 && actual != expectedLeader) {
            System.out.printf("[ПРЕДУПРЕЖДЕНИЕ] Лидер: %d, ожидался: %d%n", actual, expectedLeader);
        } else {
            System.out.printf("[OK] Выбран один лидер: узел %d%n", actual);
        }
    }

    private static void sleep(long ms) throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(ms);
    }

    private static void printHeader(int num, String desc) {
        System.out.println("\n" + "=".repeat(60));
        System.out.printf("СЦЕНАРИЙ %d: %s%n", num, desc);
        System.out.println("=".repeat(60));
    }

    private static void printFooter(int num) {
        System.out.printf("--- Сценарий %d завершён ---%n", num);
    }
}