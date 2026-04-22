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
        if (!(o instanceof Node<?> node)) {
            return false;
        }
        return Objects.equals(value, node.value) && Objects.equals(id, node.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, id);
    }
}
