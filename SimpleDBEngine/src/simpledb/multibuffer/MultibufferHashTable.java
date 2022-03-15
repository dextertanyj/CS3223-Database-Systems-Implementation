package simpledb.multibuffer;

import java.util.ArrayList;
import java.util.List;

import simpledb.record.InMemoryRecord;
import simpledb.tx.Transaction;

/**
 * This class represents a bounded size in-memory hash table.
 */
public class MultibufferHashTable {
    private List<List<InMemoryRecord>> map = new ArrayList<>();
    private int buckets;
    private int recordsize;
    private Transaction tx;
    private List<Integer> reservedpageids = new ArrayList<>();
    private int totalsize;

    public MultibufferHashTable(Transaction tx, int blockcount, int buckets) {
        this.tx = tx;
        for (int i = 0; i < blockcount; i++) {
            reservedpageids.add(tx.reserve());
        }
        for (int i = 0; i < buckets; i++) {
            map.add(new ArrayList<>());
        }
        this.totalsize = blockcount * tx.blockSize();
        this.buckets = buckets;
    }

    public void insert(int hash, InMemoryRecord record) {
        recordsize += record.getLayout().slotSize();
        if (recordsize > totalsize) {
            throw new RuntimeException("Buffer size exceeded.");
        }
        map.get(hash % buckets).add(record);
    }

    public List<InMemoryRecord> getBucket(int hash) {
        return map.get(hash % buckets);
    }

    public void close() {
        map.clear();
        for (int id : reservedpageids) {
            tx.free(id);
        }
    }
}
