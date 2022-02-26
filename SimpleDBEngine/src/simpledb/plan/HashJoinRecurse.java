package simpledb.plan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import simpledb.materialize.TempTable;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.InMemoryRecord;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class HashJoinRecurse {
    public static int IN_MEMORY_HASH_SIZE = 100;
    // TODO: Use sort merge join if max depth is exceeded.
    private static int MAX_DEPTH = 5;

    private int depth;
    private TempTable t1, t2;
    private String fldname1, fldname2;
    private Schema sch = new Schema();
    private Transaction tx;

    public HashJoinRecurse(Transaction tx, TempTable t1, TempTable t2, String fldname1, String fldname2,
            int depth) {
        this.depth = depth;
        this.tx = tx;
        this.t1 = t1;
        this.t2 = t2;
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;
        sch.addAll(t1.getLayout().schema());
        sch.addAll(t2.getLayout().schema());
    }

    public Scan open() {
        int outputBuffers = tx.availableBuffs() - 1;
        List<TempTable> t1_buckets = splitIntoBuckets(t1, fldname1, outputBuffers);
        List<TempTable> t2_buckets = splitIntoBuckets(t2, fldname2, outputBuffers);
        TempTable result = join(t1_buckets, t2_buckets);
        return result.open();
    }

    private List<TempTable> splitIntoBuckets(TempTable t, String fldname, int outputBuffers) {
        ArrayList<TempTable> buckets = new ArrayList<>();
        ArrayList<UpdateScan> scans = new ArrayList<>();
        Scan s = t.open();
        for (int i = 0; i < outputBuffers; i++) {
            TempTable table = new TempTable(tx, sch);
            buckets.add(table);
            UpdateScan scan = table.open();
            scan.beforeFirst();
            scans.add(scan);
        }
        while (s.next()) {
            int hash = s.getVal(fldname).hashCode();
            int bucket = ((int) Math.pow(hash, depth)) % outputBuffers;
            scans.get(bucket).insert();
            for (String field : t.getLayout().schema().fields()) {
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
                Scan s = new HashJoinRecurse(tx, t1_partition, t2_partition, fldname1, fldname2, depth + 1).open();
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

}
