package simpledb.record;

import java.util.HashMap;

import simpledb.query.Constant;

/**
 * This class represents a transient record kept within memory.
 */
public class InMemoryRecord {
    Layout layout;
    HashMap<String, Constant> values;

    public InMemoryRecord(Layout layout) {
        this.layout = layout;
        values = new HashMap<>();
    }

    public Constant getVal(String fldname) {
        if (!layout.schema().fields().contains(fldname)) {
            throw new RuntimeException("Field not present.");
        }
        return values.get(fldname);
    }

    public void setVal(String fldname, Constant value) {
        if (!layout.schema().fields().contains(fldname)) {
            throw new RuntimeException("Field not present.");
        }
        values.put(fldname, value);
    }

    public Layout getLayout() {
        return layout;
    }
}