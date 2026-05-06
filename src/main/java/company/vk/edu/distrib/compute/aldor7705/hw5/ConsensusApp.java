package company.vk.edu.distrib.compute.aldor7705.hw5;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public final class ConsensusApp {
    private static final Logger LOGGER = Logger.getLogger(ConsensusApp.class.getName());
    private static final int CLUSTER_SIZE = 5;

    public static void main(String[] args) throws InterruptedException {
        LOGGER.info("=".repeat(60));
        LOGGER.info("РАСПРЕДЕЛЁННЫЙ КОНСЕНСУС - АЛГОРИТМ BULLY");
        LOGGER.info("=".repeat(60));

        scenario1CleanStart();
        scenario2LeaderFailure();
        scenario3NodeRecovery();
        scenario4RapidFailures();

        LOGGER.info("\nВсе сценарии завершены!");
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
        LOGGER.info("Первоначальный лидер: " + originalLeader);

        cluster.getNode(originalLeader).forceDown();
        LOGGER.info("\n>>> Узел " + originalLeader + " (ЛИДЕР) принудительно отключён <<<\n");

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
        LOGGER.info("\n>>> Узел " + failingNode + " принудительно отключён <<<\n");

        sleep(3000);

        cluster.getNode(failingNode).forceRecover();
        LOGGER.info("\n>>> Узел " + failingNode + " принудительно восстановлен <<<\n");

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

        LOGGER.info("Начинаю цикл частых отказов и восстановлений...\n");

        for (int round = 1; round <= 5; round++) {
            int nodeId = (round % (CLUSTER_SIZE - 1)) + 1;

            LOGGER.info(String.format("Раунд %d: Отключаю узел %d...", round, nodeId));
            cluster.getNode(nodeId).forceDown();
            sleep(1000);

            LOGGER.info(String.format("Раунд %d: Восстанавливаю узел %d...", round, nodeId));
            cluster.getNode(nodeId).forceRecover();
            sleep(2000);

            cluster.printStatus();

            int leader = cluster.getConsensusLeader();
            if (leader == -2) {
                LOGGER.info("[ПРОВАЛ] Обнаружен split-brain!");
            } else if (leader < 0) {
                LOGGER.info("[ПРЕДУПРЕЖДЕНИЕ] Лидер отсутствует - выборы ещё идут");
            } else {
                LOGGER.info("[OK] Лидер: " + leader);
            }
        }

        sleep(5000);
        cluster.printStatus();

        int finalLeader = cluster.getConsensusLeader();
        if (finalLeader == -2) {
            LOGGER.info("[ПРОВАЛ] Split-brain в финальном состоянии!");
        } else if (finalLeader > 0) {
            LOGGER.info("[OK] Итоговый лидер: " + finalLeader);
        }

        cluster.shutdown();
        printFooter(4);
    }

    private static void assertLeader(Cluster cluster, int expectedLeader) {
        int actual = cluster.getConsensusLeader();
        if (actual == -2) {
            LOGGER.info("[ПРОВАЛ] Обнаружен split-brain!");
        } else if (actual < 0) {
            LOGGER.info("[ПРЕДУПРЕЖДЕНИЕ] Консенсус не достигнут");
        } else if (expectedLeader > 0 && actual != expectedLeader) {
            LOGGER.info(String.format("[ПРЕДУПРЕЖДЕНИЕ] Лидер: %d, ожидался: %d", actual, expectedLeader));
        } else {
            LOGGER.info(String.format("[OK] Выбран один лидер: узел %d", actual));
        }
    }

    private static void sleep(long ms) throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(ms);
    }

    private static void printHeader(int num, String desc) {
        LOGGER.info("\n" + "=".repeat(60));
        LOGGER.info(String.format("СЦЕНАРИЙ %d: %s", num, desc));
        LOGGER.info("=".repeat(60));
    }

    private static void printFooter(int num) {
        LOGGER.info(String.format("--- Сценарий %d завершён ---", num));
    }
}
