package simpledb.record;

import java.util.HashMap;
import java.util.List;

import simpledb.query.Constant;

/**
 * This class represents a transient record kept within memory.
 */
public class InMemoryRecord {
    Layout layout;
    List<String> fields;
    HashMap<String, Constant> values;

    public InMemoryRecord(Layout layout) {
        this.layout = layout;
        this.fields = layout.schema().fields();
        values = new HashMap<>();
    }

    public InMemoryRecord(List<String> fields) {
        this.fields = fields;
        values = new HashMap<>();
    }

    public Constant getVal(String fldname) {
        if (!fields.contains(fldname)) {
            throw new RuntimeException("Field not present.");
        }
        return values.get(fldname);
    }

    public void setVal(String fldname, Constant value) {
        if (!fields.contains(fldname)) {
            throw new RuntimeException("Field not present.");
        }
        values.put(fldname, value);
    }

    public void setFieldlist() {
        for (String field : fields) {
            values.put(field, null);
        }
    }

    public Layout getLayout() {
        return layout;
    }

    public boolean equals(InMemoryRecord other) {
        return this.values.equals(other.values);
    }

    public void putAll(InMemoryRecord other) {
        this.values.putAll(other.values);
    }
}