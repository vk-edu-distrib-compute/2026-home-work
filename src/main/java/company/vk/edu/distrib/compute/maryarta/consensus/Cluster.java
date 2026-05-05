package company.vk.edu.distrib.compute.maryarta.consensus;

import java.util.*;

public class Cluster {
    public final Map<Integer, Node> nodes;
    private List<Integer> ids;

    public Cluster(List<Integer> ids){
        this.ids = ids;
        nodes = new HashMap<>();
        create();
        start();
    }

    public Cluster(){
        ids = new ArrayList<>(Arrays.asList(1,2,3,4,5));
        nodes = new HashMap<>();
        create();
        start();
    }

    private void create(){
        for(int i: ids){
            Node node = new Node(nodes, i);
            nodes.put(i, node);
        }
    }

    private void start(){
        for(Map.Entry<Integer, Node> node : nodes.entrySet()){
            Thread thread = new Thread(node.getValue());
            thread.start();
            node.getValue().start();
        }
    }


    public void stopNode(int id){
        nodes.get(id).stop();
    }
    public void startNode(int id){
        nodes.get(id).start();
    }

}
