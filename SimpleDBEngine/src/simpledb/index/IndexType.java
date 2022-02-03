package simpledb.index;

public enum IndexType {
    TREE("tree"),
    HASH("hash");

    private final String str;

    private IndexType(String str) {
        this.str = str;
    }

    public static IndexType getType(String str) {
        for (IndexType type : IndexType.values()) {
            if (type.str.equals(str)) {
                return type;
            }
        }
        throw new RuntimeException();
    }

    public String toString() {
        return this.str;
    }
}
