package simpledb.record;

import java.util.HashMap;

import simpledb.query.Constant;

public class InMemoryRecord {
    HashMap<String, Constant> values;

    public Constant getVal(String fldname) {
        return values.get(fldname);
    }

    public void setVal(String fldname, Constant value) {
        values.put(fldname, value);
    }
}