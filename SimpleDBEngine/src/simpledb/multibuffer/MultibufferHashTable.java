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

    /**
     * Creates a multi-buffer hash table.
     * 
     * @param tx         The current transaction
     * @param blockcount The number of buffer blocks to use for the hash table
     * @param buckets    The number of buckets in the hash table.
     */
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

    /**
     * Inserts a record into the hash table.
     * 
     * @param hash   the bucket to insert into
     * @param record the record to insert
     * @throws RuntimeException if the capacity of the hash table is exceeded
     */
    public void insert(int hash, InMemoryRecord record) {
        recordsize += record.getLayout().slotSize();
        if (recordsize > totalsize) {
            throw new RuntimeException("Buffer size exceeded.");
        }
        map.get(hash % buckets).add(record);
    }

    /**
     * Returns a list of records hashed to a bucket
     * 
     * @param hash the bucket index
     * @return a list of records in the bucket
     */
    public List<InMemoryRecord> getBucket(int hash) {
        return map.get(hash % buckets);
    }

    /**
     * Closes the hash table and frees all reserved buffer pages
     */
    public void close() {
        map.clear();
        for (int id : reservedpageids) {
            tx.free(id);
        }
    }
}
