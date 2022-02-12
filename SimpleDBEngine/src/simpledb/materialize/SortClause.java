package simpledb.materialize;

import java.util.ArrayList;
import java.util.List;

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

    public static List<SortClause> generateDefaultSort(List<String> fieldnames) {
        List<SortClause> sortclauses = new ArrayList<>();
        for (String field : fieldnames) {
            sortclauses.add(new SortClause(field, SortOrder.ASCENDING));
        }
        return sortclauses;
    }
}
