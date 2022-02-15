package simpledb.materialize;

public enum SortOrder {
    ASCENDING("asc", 1),
    DESCENDING("desc", -1);

    private final String token;
    private final int modifier;

    private SortOrder(String token, int modifier) {
        this.token = token;
        this.modifier = modifier;
    }

    public int getModifier() {
        return this.modifier;
    }

    public String toString() {
        return this.token;
    }
}
