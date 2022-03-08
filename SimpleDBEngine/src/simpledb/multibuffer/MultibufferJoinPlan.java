package simpledb.multibuffer;

import simpledb.plan.Plan;
import simpledb.plan.TablePlan;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class MultibufferJoinPlan implements Plan {
    private Transaction tx;
    private TablePlan outer;
    private Plan inner;
    private String inner_field, outer_field;
    private Schema sch = new Schema();

    public MultibufferJoinPlan(Transaction tx, TablePlan outer, Plan inner, String outer_field, String inner_field) {
        this.tx = tx;
        this.outer = outer;
        this.inner = inner;
        this.outer_field = outer_field;
        this.inner_field = inner_field;
        sch.addAll(inner.schema());
        sch.addAll(outer.schema());
    }

    public Scan open() {
        Scan inner_scan = inner.open();
        return new MultibufferJoinScan(tx, outer.tblname(), outer.layout(), inner_scan, outer_field, inner_field);
    }

    public int blocksAccessed() {
        return outer.blocksAccessed()
                + ((outer.blocksAccessed() / (tx.availableBuffs() - 2)) * inner.blocksAccessed())
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
