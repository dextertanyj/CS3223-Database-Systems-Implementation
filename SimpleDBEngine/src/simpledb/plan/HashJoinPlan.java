package simpledb.plan;

import java.util.ArrayList;
import java.util.List;

import simpledb.materialize.EnhancedTempTable;
import simpledb.materialize.MergeJoinPlan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.InMemoryRecord;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class HashJoinPlan implements Plan {
    private static int IN_MEMORY_HASH_SIZE = 100;
    private static int MAX_DEPTH = 5;

    private Plan p1, p2;
    private String fldname1, fldname2;
    private Schema sch = new Schema();
    private Transaction tx;
    private int depth;

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
    }

    public Scan open() {
        // After max depth, we use sort merge join.
        if (depth > MAX_DEPTH) {
            Plan p = new MergeJoinPlan(tx, p1, p2, fldname1, fldname2);
            p.open();
        }
        int outputBuffers = tx.availableBuffs() - 1;
        List<EnhancedTempTable> p1_buckets = splitIntoBuckets(p1, fldname1, outputBuffers);
        List<EnhancedTempTable> p2_buckets = splitIntoBuckets(p2, fldname2, outputBuffers);
        EnhancedTempTable result = join(p1_buckets, p2_buckets);
        return result.open();
    }

    private List<EnhancedTempTable> splitIntoBuckets(Plan t, String fldname, int outputBuffers) {
        ArrayList<EnhancedTempTable> buckets = new ArrayList<>();
        ArrayList<UpdateScan> scans = new ArrayList<>();
        Scan s = t.open();
        for (int i = 0; i < outputBuffers; i++) {
            EnhancedTempTable table = new EnhancedTempTable(tx, t.schema());
            buckets.add(table);
            UpdateScan scan = table.open();
            scan.beforeFirst();
            scans.add(scan);
        }
        while (s.next()) {
            int hash = s.getVal(fldname).hashCode();
            int bucket = ((int) Math.pow(hash, depth)) % outputBuffers;
            scans.get(bucket).insert();
            for (String field : t.schema().fields()) {
                scans.get(bucket).setVal(field, s.getVal(field));
            }
        }
        scans.forEach(scan -> scan.close());
        s.close();
        return buckets;
    }

    private EnhancedTempTable join(List<EnhancedTempTable> p1_buckets, List<EnhancedTempTable> p2_buckets) {
        EnhancedTempTable result = new EnhancedTempTable(tx, sch);
        UpdateScan resultScan = result.open();
        resultScan.beforeFirst();
        while (p1_buckets.size() > 0) {
            EnhancedTempTable p1_partition = p1_buckets.remove(0);
            EnhancedTempTable p2_partition = p2_buckets.remove(0);
            int available_buffers = tx.availableBuffs() - 1; // Output buffer has already been allocated.
            if (tx.size(p1_partition.tableName()) > available_buffers
                    && tx.size(p2_partition.tableName()) > available_buffers) {
                Scan s = new HashJoinPlan(tx, p1_partition, p2_partition, fldname1, fldname2, depth + 1).open();
                s.beforeFirst();
                while (s.next()) {
                    resultScan.insert();
                    for (String fldname : sch.fields()) {
                        resultScan.setVal(fldname, s.getVal(fldname));
                    }
                }
                s.close();
            } else if (tx.size(p1_partition.tableName()) > available_buffers) {
                hashAndJoin(p2_partition, p1_partition, fldname2, fldname1, resultScan);
            } else {
                hashAndJoin(p1_partition, p2_partition, fldname1, fldname2, resultScan);
            }
        }
        resultScan.close();
        return result;
    }

    private void hashAndJoin(EnhancedTempTable outerTable, EnhancedTempTable innerTable, String outerJoinFld,
            String innerJoinFld,
            UpdateScan result) {
        Scan s = outerTable.open();
        s.beforeFirst();
        List<List<InMemoryRecord>> map = new ArrayList<>();
        for (int i = 0; i < IN_MEMORY_HASH_SIZE; i++) {
            map.add(new ArrayList<>());
        }
        while (s.next()) {
            int hash = ((int) Math.pow(s.getVal(outerJoinFld).hashCode(), depth + 1)) % IN_MEMORY_HASH_SIZE;
            InMemoryRecord record = new InMemoryRecord();
            for (String fldname : outerTable.getLayout().schema().fields()) {
                record.setVal(fldname, s.getVal(fldname));
            }
            map.get(hash).add(record);
        }
        s.close();
        s = innerTable.open();
        s.beforeFirst();
        while (s.next()) {
            int hash = ((int) Math.pow(s.getVal(innerJoinFld).hashCode(), depth + 1)) % IN_MEMORY_HASH_SIZE;
            for (InMemoryRecord record : map.get(hash)) {
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
    }

    public int blocksAccessed() {
        // TODO: Fill in blocks accessed count.
        return 0;
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
}
