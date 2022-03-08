package simpledb.materialize;

import java.util.Optional;

import simpledb.plan.Plan;
import simpledb.plan.QueryPlanPrinter;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

public class EnhancedTempTable extends TempTable implements Plan {
    Schema sch;
    // Plan p1, p2;
    Optional<Integer> numblocks;
    Optional<Integer> numrecs;

    public EnhancedTempTable(Transaction tx, Schema sch) {
        super(tx, sch);
        this.sch = sch;
        // this.p1 = p1;
        // this.p2 = p2;
    }

    public int blocksAccessed() {
        if (numblocks.isEmpty()) {
            updateStats();
        }
        return numblocks.get();
    }

    public int recordsOutput() {
        if (numrecs.isEmpty()) {
            updateStats();
        }
        return numrecs.get();
    }

    /**
     * Return the estimated number of distinct values
     * for the specified field.
     * This estimate is a complete guess, because doing something
     * reasonable is beyond the scope of this system.
     * 
     * @param fldname the name of the field
     * @return a guess as to the number of distinct field values
     */
    public int distinctValues(String fldname) {
        return 1 + (recordsOutput() / 3);
    }

    public Schema schema() {
        return sch;
    }

    private void updateStats() {
        TableScan s = (TableScan) super.open();
        int reccount = 0;
        int blockcount = 0;
        while (s.next()) {
            reccount++;
            blockcount = s.getRid().blockNumber() + 1;
        }
        s.close();
        numrecs = Optional.of(reccount);
        numblocks = Optional.of(blockcount);
    }

    //TODO: verify correctness
    public QueryPlanPrinter getPlanDesc() {
        // QueryPlanPrinter printer = QueryPlanPrinter.getJoinPlanPrinter(p1.getPlanDesc(), p2.getPlanDesc());
        // return printer;
        return null;
    }
}