package simpledb.plan;

import java.util.ArrayList;
import java.util.List;

import simpledb.materialize.TempTable;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.InMemoryRecord;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class HashJoinPlan implements Plan {
    private Plan p1, p2;
    private String fldname1, fldname2;
    private Schema sch = new Schema();
    private Transaction tx;

    public HashJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2) {
        this.tx = tx;
        this.p1 = p1;
        this.p2 = p2;
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;
        sch.addAll(p1.schema());
        sch.addAll(p2.schema());
    }

    public Scan open() {
        int outputBuffers = tx.availableBuffs() - 1;
        List<TempTable> outer_buckets = splitIntoBuckets(p1, fldname1, outputBuffers);
        List<TempTable> inner_buckets = splitIntoBuckets(p2, fldname2, outputBuffers);
        TempTable result = join(outer_buckets, inner_buckets);
        return result.open();
    }

    private List<TempTable> splitIntoBuckets(Plan p, String fldname, int outputBuffers) {
        ArrayList<TempTable> buckets = new ArrayList<>();
        ArrayList<UpdateScan> scans = new ArrayList<>();
        Scan s = p.open();
        for (int i = 0; i < outputBuffers; i++) {
            TempTable table = new TempTable(tx, p.schema());
            buckets.add(table);
            UpdateScan scan = table.open();
            scan.beforeFirst();
            scans.add(scan);
        }
        while (s.next()) {
            int hash = s.getVal(fldname).hashCode();
            int bucket = hash % outputBuffers;
            scans.get(bucket).insert();
            for (String field : p.schema().fields()) {
                scans.get(bucket).setVal(field, s.getVal(field));
            }
        }
        scans.forEach(scan -> scan.close());
        s.close();
        return buckets;
    }

    private TempTable join(List<TempTable> t1_buckets, List<TempTable> t2_buckets) {
        TempTable result = new TempTable(tx, sch);
        UpdateScan resultScan = result.open();
        resultScan.beforeFirst();
        while (t1_buckets.size() > 0) {
            TempTable t1_partition = t1_buckets.remove(0);
            TempTable t2_partition = t2_buckets.remove(0);
            int available_buffers = tx.availableBuffs() - 1; // Output buffer has already been allocated.
            if (tx.size(t1_partition.tableName()) > available_buffers
                    && tx.size(t2_partition.tableName()) > available_buffers) {
                Scan s = new HashJoinRecurse(tx, t1_partition, t2_partition, fldname1, fldname2, 2).open();
                s.beforeFirst();
                while (s.next()) {
                    resultScan.insert();
                    for (String fldname : sch.fields()) {
                        resultScan.setVal(fldname, s.getVal(fldname));
                    }
                }
                s.close();
            } else if (tx.size(t1_partition.tableName()) > available_buffers) {
                hashAndJoin(t2_partition, t1_partition, fldname2, fldname1, resultScan);
            } else {
                hashAndJoin(t1_partition, t2_partition, fldname1, fldname2, resultScan);
            }
        }
        resultScan.close();
        return result;
    }

    private void hashAndJoin(TempTable outerTable, TempTable innerTable, String outerJoinFld, String innerJoinFld,
            UpdateScan result) {
        Scan s = outerTable.open();
        s.beforeFirst();
        List<List<InMemoryRecord>> map = new ArrayList<>();
        for (int i = 0; i < HashJoinRecurse.IN_MEMORY_HASH_SIZE; i++) {
            map.add(new ArrayList<>());
        }
        while (s.next()) {
            int hash = ((int) Math.pow(s.getVal(outerJoinFld).hashCode(), 2)) % HashJoinRecurse.IN_MEMORY_HASH_SIZE;
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
            int hash = ((int) Math.pow(s.getVal(innerJoinFld).hashCode(), 2)) % HashJoinRecurse.IN_MEMORY_HASH_SIZE;
            for (InMemoryRecord record : map.get(hash)) {
                if (s.getVal(innerJoinFld) == record.getVal(outerJoinFld)) {
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
