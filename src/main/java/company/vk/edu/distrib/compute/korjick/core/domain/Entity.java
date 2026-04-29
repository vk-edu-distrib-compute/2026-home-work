package company.vk.edu.distrib.compute.korjick.core.domain;

public class Entity {
    private final Key key;
    private final byte[] body;
    private final long version;
    private final boolean deleted;

    public Entity(Key key, byte[] body, long version, boolean deleted) {
        this.key = key;
        this.body = body == null ? new byte[0] : body.clone();
        this.version = version;
        this.deleted = deleted;
    }

    public Key key() {
        return key;
    }

    public byte[] body() {
        return body.clone();
    }

    public long version() {
        return version;
    }

    public boolean deleted() {
        return deleted;
    }

    public record Key(String value) {
        public Key {
            if (value == null || value.isEmpty()) {
                throw new IllegalArgumentException("Key must not be null or empty");
            }
        }
    }
}
