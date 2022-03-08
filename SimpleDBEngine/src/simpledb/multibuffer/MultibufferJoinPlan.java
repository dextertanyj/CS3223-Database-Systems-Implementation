package simpledb.multibuffer;

import simpledb.plan.Plan;
import simpledb.plan.TablePlan;
import simpledb.plan.QueryPlanPrinter;
import simpledb.query.Scan;
import simpledb.query.Term;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class MultibufferJoinPlan implements Plan {
    private Transaction tx;
    private TablePlan outer;
    private Plan inner;
    Term term;
    private Schema sch = new Schema();

    public MultibufferJoinPlan(Transaction tx, TablePlan outer, Plan inner, Term term) {
        this.tx = tx;
        this.outer = outer;
        this.inner = inner;
        this.term = term;
        sch.addAll(inner.schema());
        sch.addAll(outer.schema());
    }

    public Scan open() {
        Scan inner_scan = inner.open();
        return new MultibufferJoinScan(tx, outer.tblname(), outer.layout(), inner_scan, term);
    }

    public int blocksAccessed() {
        return outer.blocksAccessed()
                + ((outer.blocksAccessed() / (tx.availableBuffs() - 2)) * inner.blocksAccessed())
                + recordsOutput();
    }

    public int recordsOutput() {
        String innerfld = "", outerfld = "";
        for (String str : inner.schema().fields()) {
            String s = term.comparesWithField(str);
            if (s != null) {
                innerfld = str;
                outerfld = s;
            }
        }
        int maxvals = Math.max(inner.distinctValues(innerfld),
                outer.distinctValues(outerfld));
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

    public QueryPlanPrinter getPlanDesc() {
        QueryPlanPrinter printer = QueryPlanPrinter.getJoinPlanPrinter(inner.getPlanDesc(), outer.getPlanDesc());
        String toAdd = QueryPlanPrinter.getJoinPlanDesc("Multibuffer join plan", inner_field, outer_field);
        return printer.add(toAdd);
    }
}
