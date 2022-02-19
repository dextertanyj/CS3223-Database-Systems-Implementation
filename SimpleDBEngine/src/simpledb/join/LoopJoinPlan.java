package simpledb.join;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.record.TableScan;

public class LoopJoinPlan implements Plan {
    private Plan inner, outer;
    private String inner_field, outer_field;
    private Schema sch = new Schema();

    public LoopJoinPlan(Plan inner, Plan outer, String inner_field, String outer_field) {
        this.inner = inner;
        this.outer = outer;
        this.inner_field = inner_field;
        this.outer_field = outer_field;
        sch.addAll(inner.schema());
        sch.addAll(outer.schema());
    }

    public Scan open() {
        TableScan inner_scan = (TableScan) inner.open();
        TableScan outer_scan = (TableScan) outer.open();
        return new LoopJoinScan(inner_scan, outer_scan, inner_field, outer_field);
    }

    public int blocksAccessed() {
        return outer.blocksAccessed()
                + (outer.blocksAccessed() * inner.blocksAccessed())
                + recordsOutput();
    }

    public int recordsOutput() {
        int maxvals = Math.max(inner.distinctValues(inner_field),
                outer.distinctValues(outer_field));
        return (inner.recordsOutput() * outer.recordsOutput()) / maxvals;
    }

    /**
     * Estimates the number of distinct values for the
     * specified field.
     * 
     * @see simpledb.plan.Plan#distinctValues(java.lang.String)
     */
    public int distinctValues(String fldname) {
        if (inner.schema().hasField(fldname))
            return inner.distinctValues(fldname);
        else
            return outer.distinctValues(fldname);
    }

    /**
     * Returns the schema of the index join.
     * 
     * @see simpledb.plan.Plan#schema()
     */
    public Schema schema() {
        return sch;
    }
}
