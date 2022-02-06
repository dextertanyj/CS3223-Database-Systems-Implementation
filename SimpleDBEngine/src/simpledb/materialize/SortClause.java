package simpledb.materialize;

public class SortClause {
    private String fieldname;
    private SortOrder order;

    public SortClause(String fieldname, SortOrder order) {
        this.fieldname = fieldname;
        this.order = order;
    }

    public String getField() {
        return this.fieldname;
    }

    public SortOrder getOrder() {
        return this.order;
    }

    public String toString() {
        return this.fieldname + " " + this.order;
    }
}
