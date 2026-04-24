package company.vk.edu.distrib.compute.andeco.sharding;

import java.util.Objects;

public class Node<T> {
    private T value;
    private String id;

    public Node(T value, String id) {
        this.value = value;
        this.id = id;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public void setId(String id) {
        this.id = id;
    }

    public T getValue() {
        return value;
    }

    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Node<?> node
                && Objects.equals(id, node.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
