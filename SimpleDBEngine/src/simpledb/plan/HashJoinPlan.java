package simpledb.plan;

import java.util.ArrayList;
import java.util.List;

import simpledb.materialize.EnhancedTempTable;
import simpledb.materialize.MaterializePlan;
import simpledb.materialize.MergeJoinPlan;
import simpledb.multibuffer.MultibufferHashTable;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.InMemoryRecord;
import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

/**
 * The Plan class for the hash join operator.
 * 
 * This hash join operator does not guarentee which of its plans are used as the
 * build and probe tables. The left-hand plan is preferentially chosen as the
 * build table but if a partition size exceeds, the right hand plan may be used
 * as the build table.
 */

public class HashJoinPlan implements Plan {
    private static int IN_MEMORY_HASH_SIZE = 100;
    private static int MAX_DEPTH = 3;
    private static int SEED = 0x9e3779b9;

    private Plan p1, p2;
    private String fldname1, fldname2;
    private Schema sch = new Schema();
    private Transaction tx;
    private int depth;
    private EnhancedTempTable resulttable;

    /**
     * Creates a new hash join plan using the specified plans joined on the
     * specified fields.
     * 
     * @param tx       the calling transaction
     * @param p1       the left-hand plan
     * @param p2       the right-hand plan
     * @param fldname1 the left-hand field used for joining
     * @param fldname2 the right-hand field used for joining
     */
    public HashJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2) {
        this(tx, p1, p2, fldname1, fldname2, 1);
    }

    /**
     * Creates a recursive hash join plan using the specified plans joined on the
     * specified fields.
     * 
     * @param tx       the calling transaction
     * @param p1       the left-hand plan
     * @param p2       the right-hand plan
     * @param fldname1 the left-hand field used for joining
     * @param fldname2 the right-hand field used for joining
     * @param depth    the recursion depth of this call
     */
    private HashJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2, int depth) {
        this.tx = tx;
        this.p1 = p1;
        this.p2 = p2;
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;
        this.depth = depth;
        sch.addAll(p1.schema());
        sch.addAll(p2.schema());
        this.resulttable = new EnhancedTempTable(tx, sch);
    }

    /**
     * Creates a recursive hash join plan using the specified plans joined on the
     * specified fields with the specified result table.
     * 
     * This overloaded constructed reduces IO overhead as results are appended to
     * the result table directly without the need for copying.
     * 
     * @param tx          the calling transaction
     * @param p1          the left-hand plan
     * @param p2          the right-hand plan
     * @param fldname1    the left-hand field used for joining
     * @param fldname2    the right-hand field used for joining
     * @param depth       the recursion depth of this call
     * @param resulttable the result table of the calling hash join plan
     */
    private HashJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2, int depth,
            EnhancedTempTable resulttable) {
        this.tx = tx;
        this.p1 = p1;
        this.p2 = p2;
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;
        this.depth = depth;
        sch.addAll(p1.schema());
        sch.addAll(p2.schema());
        this.resulttable = resulttable;
    }

    public UpdateScan open() {
        // After max depth, we use sort merge join.
        if (depth > MAX_DEPTH) {
            Plan p = new MergeJoinPlan(tx, p1, p2, fldname1, fldname2);
            copyScan(p.open());
        } else {
            int outputBuffers = tx.availableBuffs() - 1;
            // Materialize the input tables to ensure we have sufficient buffers during
            // parititoning.
            List<EnhancedTempTable> p1_buckets = splitIntoBuckets(new MaterializePlan(tx, p1), fldname1, outputBuffers);
            List<EnhancedTempTable> p2_buckets = splitIntoBuckets(new MaterializePlan(tx, p2), fldname2, outputBuffers);
            join(p1_buckets, p2_buckets);
        }
        return resulttable.open();
    }

    /**
     * Splits the records in provided plan into buckets.
     * 
     * @param t             the plan to split
     * @param fldname       the fieldname to hash
     * @param outputBuffers the number of output buffers available for use.
     * @return a list of buckets containing the records of t.
     */
    private List<EnhancedTempTable> splitIntoBuckets(Plan t, String fldname, int outputBuffers) {
        ArrayList<EnhancedTempTable> buckets = new ArrayList<>();
        ArrayList<UpdateScan> scans = new ArrayList<>();
        Scan s = t.open();
        for (int i = 0; i < outputBuffers; i++) {
            EnhancedTempTable table = new EnhancedTempTable(tx, t.schema());
            buckets.add(table);
            UpdateScan scan = table.open();
            scans.add(scan);
        }
        while (s.next()) {
            int hash = hash(s.getVal(fldname).hashCode(), depth);
            int bucket = hash % outputBuffers;
            scans.get(bucket).insert();
            for (String field : t.schema().fields()) {
                scans.get(bucket).setVal(field, s.getVal(field));
            }
        }
        scans.forEach(scan -> scan.close());
        s.close();
        return buckets;
    }

    /**
     * Joins the respective buckets of the provided lists of buckets and stores the
     * resulting records into the resulttable of this join.
     * 
     * @param p1_buckets the buckets of the left-hand plan.
     * @param p2_buckets the buckets of the right-hand plan.
     */
    private void join(List<EnhancedTempTable> p1_buckets, List<EnhancedTempTable> p2_buckets) {
        UpdateScan resultScan = null;
        while (p1_buckets.size() > 0) {
            EnhancedTempTable p1_partition = p1_buckets.remove(0);
            EnhancedTempTable p2_partition = p2_buckets.remove(0);
            int available_buffers = tx.availableBuffs() - 2;
            int p1_size = tx.size(p1_partition.tableName() + ".tbl");
            int p2_size = tx.size(p2_partition.tableName() + ".tbl");
            if (p1_size > available_buffers
                    && p2_size > available_buffers) {
                resultScan = new HashJoinPlan(tx, p1_partition, p2_partition, fldname1, fldname2, depth + 1,
                        resulttable).open();
            } else if (p1_size > available_buffers) {
                resultScan = resulttable.open();
                hashAndJoin(p2_partition, p1_partition, fldname2, fldname1, resultScan,
                        Math.min(p2_size, available_buffers));
            } else {
                resultScan = resulttable.open();
                hashAndJoin(p1_partition, p2_partition, fldname1, fldname2, resultScan,
                        Math.min(p1_size, available_buffers));
            }
            resultScan.close();
        }
    }

    /**
     * Constructs a hash table of records from the build table and probes it with
     * records from the probe table.
     * 
     * @param outerTable   the build table
     * @param innerTable   the probe table
     * @param outerJoinFld the build table field used for joining
     * @param innerJoinFld the probe table field used for joining
     * @param result       the result table to store joined records
     * @param buffer_count the number of buffers to reserve for the in memory hash
     *                     table
     */
    private void hashAndJoin(EnhancedTempTable outerTable, EnhancedTempTable innerTable, String outerJoinFld,
            String innerJoinFld,
            UpdateScan result, int buffer_count) {
        Scan s = outerTable.open();
        MultibufferHashTable hashtable = new MultibufferHashTable(tx, buffer_count, IN_MEMORY_HASH_SIZE);
        while (s.next()) {
            int hash = hash(s.getVal(outerJoinFld).hashCode(), depth + 1);
            InMemoryRecord record = new InMemoryRecord(outerTable.getLayout());
            for (String fldname : outerTable.getLayout().schema().fields()) {
                record.setVal(fldname, s.getVal(fldname));
            }
            hashtable.insert(hash, record);
        }
        s.close();
        s = innerTable.open();
        while (s.next()) {
            int hash = hash(s.getVal(innerJoinFld).hashCode(), depth + 1);
            for (InMemoryRecord record : hashtable.getBucket(hash)) {
                if (s.getVal(innerJoinFld).equals(record.getVal(outerJoinFld))) {
                    result.insert();
                    for (String fldname : sch.fields()) {
                        if (s.hasField(fldname)) {
                            result.setVal(fldname, s.getVal(fldname));
                        } else {
                            result.setVal(fldname, record.getVal(fldname));
                        }
                    }
                }
            }
        }
        hashtable.close();
        s.close();
    }

    private void copyScan(Scan s) {
        UpdateScan result = resulttable.open();
        while (s.next()) {
            result.insert();
            for (String fldname : sch.fields()) {
                result.setVal(fldname, s.getVal(fldname));
            }
        }
        result.close();
        s.close();
    }

    /**
     * Returns an estimate of the number of block accesses
     * required to execute the query.
     * 
     * The method uses the current number of available buffers and assumes uniform
     * distribution of records into buckets to calculate the number of blocks of
     * each parition, and so this value may differ when the query scan is opened.
     * 
     * The method also does not account for if the max recursion depth is reached.
     * 
     * @see simpledb.plan.Plan#blocksAccessed()
     */
    public int blocksAccessed() {
        Layout p1_layout = new Layout(p1.schema());
        Layout p2_layout = new Layout(p2.schema());
        int cost = (p1.blocksAccessed() + p2.blocksAccessed()) * 2;
        int p1_blocks = p1.recordsOutput() / (tx.blockSize() / p1_layout.slotSize());
        int p2_blocks = p2.recordsOutput() / (tx.blockSize() / p2_layout.slotSize());
        int estimated_buffers = tx.availableBuffs();
        int min_blocks = Math.min(p1_blocks, p2_blocks);
        int max_blocks = Math.max(p1_blocks, p2_blocks);
        int temp = (int) Math.ceil(min_blocks / estimated_buffers);
        while (temp > estimated_buffers) {
            cost += min_blocks * 2;
            temp = (int) Math.ceil(temp / estimated_buffers);
        }
        cost += max_blocks;
        return cost;
    }

    /**
     * Estimates the number of output records in the join.
     * 
     * @see simpledb.plan.Plan#recordsOutput()
     */
    public int recordsOutput() {
        int maxvals = Math.max(p1.distinctValues(fldname1),
                p2.distinctValues(fldname2));
        return (p1.recordsOutput() * p2.recordsOutput()) / maxvals;
    }

    /**
     * Estimate the distinct number of field values in the join.
     * Since the join does not increase or decrease field values,
     * the estimate is the same as in the appropriate underlying query.
     * 
     * @see simpledb.plan.Plan#distinctValues(java.lang.String)
     */
    public int distinctValues(String fldname) {
        if (p1.schema().hasField(fldname))
            return p1.distinctValues(fldname);
        else
            return p2.distinctValues(fldname);
    }

    /**
     * Return the schema of the join,
     * which is the union of the schemas of the underlying queries.
     * 
     * @see simpledb.plan.Plan#schema()
     */
    public Schema schema() {
        return sch;
    }

    public QueryPlanPrinter getPlanDesc() {
        QueryPlanPrinter printer = QueryPlanPrinter.getJoinPlanPrinter(p1.getPlanDesc(), p2.getPlanDesc());
        String toAdd = QueryPlanPrinter.getJoinPlanDesc("Hash join", fldname1, fldname2);
        return printer.add(toAdd);
    }

    private int hash(int value, int depth) {
        return Math.abs((SEED >> depth) ^ value);
    }
}
