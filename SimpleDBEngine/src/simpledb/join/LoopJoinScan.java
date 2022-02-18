package simpledb.join;

import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.record.TableScan;

public class LoopJoinScan implements Scan {
    private TableScan inner;
    private TableScan outer;
    private String inner_field, outer_field;

    public LoopJoinScan(TableScan inner, TableScan outer, String inner_field, String outer_field) {
        this.inner = inner;
        this.outer = outer;
        this.inner_field = inner_field;
        this.outer_field = outer_field;
    }

    public void beforeFirst() {
        outer.beforeFirst();
        outer.next();
        inner.beforeFirst();
        inner.next();
    }

    public boolean next() {
        while (true) {
            if (inner.atLastSlot()) {
                if (outer.atLastSlot()) {
                    if (inner.next()) {
                        outer.resetSlot();
                    } else {
                        inner.close();
                        if (outer.next()) {
                            inner.beforeFirst();
                            inner.next();
                        } else {
                            outer.close();
                            return false;
                        }
                    }
                } else {
                    inner.resetSlot();
                    outer.next();
                }
            } else {
                inner.next();
            }
            if (inner.getVal(inner_field).equals(outer.getVal(outer_field))) {
                return true;
            }
        }
    }

    public int getInt(String fldname) {
        if (inner.hasField(fldname)) {
            return inner.getInt(fldname);
        }
        return outer.getInt(fldname);
    }

    public Constant getVal(String fldname) {
        if (inner.hasField(fldname)) {
            return inner.getVal(fldname);
        }
        return outer.getVal(fldname);
    }

    public String getString(String fldname) {
        if (inner.hasField(fldname)) {
            return inner.getString(fldname);
        }
        return outer.getString(fldname);
    }

    public boolean hasField(String fldname) {
        return inner.hasField(fldname) || outer.hasField(fldname);
    }

    public void close() {
        inner.close();
        outer.close();
    }
}
