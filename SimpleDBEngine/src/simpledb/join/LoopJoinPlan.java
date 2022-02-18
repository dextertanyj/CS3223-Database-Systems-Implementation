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

    /**
     * Opens an indexjoin scan for this query
     * 
     * @see simpledb.plan.Plan#open()
     */
    public Scan open() {
        TableScan inner_scan = (TableScan) inner.open();
        TableScan outer_scan = (TableScan) outer.open();
        return new LoopJoinScan(inner_scan, outer_scan, inner_field, outer_field);
    }

    /**
     * Estimates the number of block accesses to compute the join.
     * The formula is:
     * 
     * <pre>
     *  B(indexjoin(p1,p2,idx)) = B(p1) + R(p1)*B(idx)
     *       + R(indexjoin(p1,p2,idx)
     * </pre>
     * 
     * @see simpledb.plan.Plan#blocksAccessed()
     */
    public int blocksAccessed() {
        return outer.blocksAccessed()
                + (outer.blocksAccessed() * inner.blocksAccessed())
                + recordsOutput();
    }

    /**
     * Estimates the number of output records in the join.
     * The formula is:
     * 
     * <pre>
     * R(indexjoin(p1, p2, idx)) = R(p1) * R(idx)
     * </pre>
     * 
     * @see simpledb.plan.Plan#recordsOutput()
     */
    public int recordsOutput() {
        return inner.recordsOutput() * outer.recordsOutput();
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
