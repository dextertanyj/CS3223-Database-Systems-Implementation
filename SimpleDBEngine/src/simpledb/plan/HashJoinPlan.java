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

    public HashJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2) {
        this(tx, p1, p2, fldname1, fldname2, 1);
    }

    public HashJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2, int depth) {
        this.tx = tx;
        this.p1 = p1;
        this.p2 = p2;
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;
        this.depth = depth;
        sch.addAll(p1.schema());
        sch.addAll(p2.schema());
        this.resulttable = new EnhancedTempTable(tx, sch, p1, p2);
    }

    public HashJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2, int depth,
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

    private List<EnhancedTempTable> splitIntoBuckets(Plan t, String fldname, int outputBuffers) {
        ArrayList<EnhancedTempTable> buckets = new ArrayList<>();
        ArrayList<UpdateScan> scans = new ArrayList<>();
        Scan s = t.open();
        for (int i = 0; i < outputBuffers; i++) {
            EnhancedTempTable table = new EnhancedTempTable(tx, t.schema(), t, null);
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

    public int recordsOutput() {
        int maxvals = Math.max(p1.distinctValues(fldname1),
                p2.distinctValues(fldname2));
        return (p1.recordsOutput() * p2.recordsOutput()) / maxvals;
    }

    public int distinctValues(String fldname) {
        if (p1.schema().hasField(fldname))
            return p1.distinctValues(fldname);
        else
            return p2.distinctValues(fldname);
    }

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
