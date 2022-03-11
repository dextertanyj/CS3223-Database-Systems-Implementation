package simpledb.index;

/**
 * Represents the various types of indexes.
 */
public enum IndexType {
    TREE("btree"),
    HASH("hash");

    private final String str;

    private IndexType(String str) {
        this.str = str;
    }

    /**
     * Creates the corresponding IndexType based on the input string.
     * 
     * @param str the input to create the corresponding IndexType.
     * @return the corresponding IndexType.
     */
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
