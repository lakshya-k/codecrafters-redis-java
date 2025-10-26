public class Value<V> {
    private final ValueType type;
    private final V value;
    private final Long expiry;

    public Value(ValueType type, V value, Long expiry) {
        this.type = type;
        this.value = value;
        this.expiry = expiry;
    }

    public ValueType getValueType() {
        return type;
    }

    public V getValue() {
        return value;
    }

    public Long getExpiry() {
        return expiry;
    }

    @Override
    public String toString() {
        return "(" + type + ", " + value + ", " + expiry + ")";
    }
}
