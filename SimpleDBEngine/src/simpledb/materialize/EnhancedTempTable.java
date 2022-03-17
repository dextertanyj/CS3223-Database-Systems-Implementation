package simpledb.materialize;

import java.util.Optional;

import simpledb.plan.Plan;
import simpledb.plan.QueryPlanPrinter;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

/**
 * This class extends the TempTable class to comply with the Plan interface.
 */
public class EnhancedTempTable extends TempTable implements Plan {
    Schema sch;
    Optional<Integer> numblocks = Optional.empty();
    Optional<Integer> numrecs = Optional.empty();

    /**
     * Create an EnchancedTempTable with the provided schema
     * 
     * @param tx  the current transaction
     * @param sch the schema of the table
     */
    public EnhancedTempTable(Transaction tx, Schema sch) {
        super(tx, sch);
        this.sch = sch;
    }

    /**
     * Returns the number of blocks in this EnhancedTempTable.
     * 
     * @return the number of blocks in this EnhancedTempTable.
     */
    public int blocksAccessed() {
        if (numblocks.isEmpty()) {
            updateStats();
        }
        return numblocks.get();
    }

    /**
     * Returns the number of records in this EnhancedTempTable.
     * 
     * @return the number of records in this EnhancedTempTable.
     */
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

    /**
     * Returns the schema of this EnhancedTempTable.
     * 
     * @return the schema of this EnhancedTempTable.
     */
    public Schema schema() {
        return sch;
    }

    /**
     * Calculate and cache the statistics of this EnhancedTempTable.
     */
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

    public QueryPlanPrinter getPlanDesc() {
        return null;
    }
}