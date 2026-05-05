package company.vk.edu.distrib.compute;

import company.vk.edu.distrib.compute.maryarta.consensus.Cluster;
import org.checkerframework.checker.units.qual.C;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

public class ConsensusTest {

    @Test
    public void testLeaderID() throws InterruptedException {
        List<Integer> ids = getRandomId(4);
        int max = ids.stream().max(Integer::compareTo).get();
        Cluster cluster = new Cluster(ids);
        Thread.sleep(10_000);

        Assertions.assertEquals(max, cluster.nodes.get(ids.getFirst()).getLeaderID());

        cluster.stopNode(max);

        Logger log = LoggerFactory.getLogger("node");
        log.info("node {} was stopped", max);
        Thread.sleep(20_000);
        int min = ids.stream().min(Integer::compareTo).get();
        int secondMax = ids.stream()
                .distinct()
                .sorted(Comparator.reverseOrder())
                .skip(1)
                .findFirst()
                .orElseThrow();
        log.info("node {} min ", min);

        Assertions.assertEquals(secondMax, cluster.nodes.get(min).getLeaderID());

        cluster.startNode(max);
        log.info("node {} was started", max);
        Thread.sleep(10_000);
        Assertions.assertEquals(max, cluster.nodes.get(ids.getFirst()).getLeaderID());

    }
//    @Test
//    public void randomNodeStop(){
//
//    }


    private List<Integer> getRandomId(int n){
        Random random = new Random();
        List<Integer> ids = new ArrayList<>();
        for(int i = 0; i<n; i++){
            ids.add(random.nextInt(100));
        }
        return ids;
    }


}
