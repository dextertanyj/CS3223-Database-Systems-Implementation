package simpledb.multibuffer;

import simpledb.materialize.MaterializePlan;
import simpledb.plan.Plan;
import simpledb.plan.TablePlan;
import simpledb.plan.QueryPlanPrinter;
import simpledb.query.Scan;
import simpledb.query.Term;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

/**
 * The Plan class for the multi-buffer version of the
 * nested loop join operator.
 */
public class MultibufferJoinPlan implements Plan {
    private Transaction tx;
    private TablePlan outer;
    private Plan inner;
    Term term;
    private Schema sch = new Schema();

    /**
     * Creates a multi-buffer nested loop join plan for the specified queries.
     * 
     * @param tx    the calling transaction
     * @param outer the table plan for the outer query
     * @param inner the plan for the inner query
     * @param term  the term to perform the join comparison on
     */
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

    /**
     * Returns an estimate of the number of block accesses
     * required to execute the query. The formula is:
     * 
     * <pre>
     * B(blockjoin(p1, p2)) = B(p1) + C(p1) * B(p2)
     * </pre>
     * 
     * where C(p1) is the number of chunks of p1.
     * The method uses the current number of available buffers
     * to calculate C(p1), and so this value may differ
     * when the query scan is opened.
     * 
     * @see simpledb.plan.Plan#blocksAccessed()
     */
    public int blocksAccessed() {
        int avail = tx.availableBuffs();
        int size = new MaterializePlan(tx, outer).blocksAccessed();
        int numchunks = (int) Math.ceil(size / (double) avail);
        return outer.blocksAccessed()
                + (numchunks * inner.blocksAccessed());
    }

    /**
     * Estimates the number of output records in the join.
     * 
     * @see simpledb.plan.Plan#recordsOutput()
     */
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
     * Returns the schema of the multi-buffer join.
     * 
     * @see simpledb.plan.Plan#schema()
     */
    public Schema schema() {
        return sch;
    }

    public QueryPlanPrinter getPlanDesc() {
        QueryPlanPrinter printer = QueryPlanPrinter.getJoinPlanPrinter(inner.getPlanDesc(), outer.getPlanDesc());
        String toAdd = String.format("Multibuffer join on: [%s]", term.toString());
        return printer.add(toAdd);
    }
}
