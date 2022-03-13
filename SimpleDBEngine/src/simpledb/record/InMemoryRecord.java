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

    /**
     * Creates an InMemoryRecord with the given layout
     * 
     * @param layout the layout of the InMemoryRecord
     */
    public InMemoryRecord(Layout layout) {
        this.layout = layout;
        this.fields = layout.schema().fields();
        values = new HashMap<>();
        setFieldlist();
    }

    /**
     * Creates an InMemoryRecord with the given set of fields
     * The size of the InMemoryRecord is not obtainable with this constructor
     * 
     * @param fields the fields of the InMemoryRecord
     */
    public InMemoryRecord(List<String> fields) {
        this.fields = fields;
        values = new HashMap<>();
        setFieldlist();
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

    public Layout getLayout() {
        return layout;
    }

    /**
     * Returns true if two InMemoryRecords have the same fields and values,
     * otherwise false.
     * 
     * @param other the other InMemoryRecord
     * @return true if two InMemoryRecords have the same fields and values,
     *         otherwise false
     */
    public boolean equals(InMemoryRecord other) {
        return this.values.equals(other.values);
    }

    /**
     * Copies the contents of one InMemoryRecord to another.
     * 
     * @param other the InMemoryRecord to copy from.
     */
    public void putAll(InMemoryRecord other) {
        this.values.putAll(other.values);
    }

    /**
     * Initializes all fields to null.
     */
    private void setFieldlist() {
        for (String field : fields) {
            values.put(field, null);
        }
    }

}