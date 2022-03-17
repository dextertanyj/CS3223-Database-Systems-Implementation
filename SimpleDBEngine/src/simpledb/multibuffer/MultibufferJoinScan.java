package simpledb.multibuffer;

import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.Term;
import simpledb.record.Layout;
import simpledb.tx.Transaction;

/**
 * The Scan class for the multi-buffer version of the
 * nested loop join operator.
 */
public class MultibufferJoinScan implements Scan {
    private Transaction tx;
    private String filename;
    private Layout outerlayout;
    private Scan outer, inner;
    private Term term;
    private int chunksize, nextblknum, filesize;
    private boolean first = true;

    /**
     * Creates the scan class for the specified queries.
     * 
     * @param tx      the current transaction
     * @param tblname the name of the table of the outer query
     * @param layout  the metadata for the outer query
     * @param inner   the scan for the inner query
     * @param term    the term to perform the join comparison on
     */
    public MultibufferJoinScan(Transaction tx, String tblname, Layout layout, Scan inner, Term term) {
        this.tx = tx;
        this.inner = inner;
        this.filename = tblname + ".tbl";
        this.outerlayout = layout;
        this.term = term;
        this.filesize = tx.size(filename);
        this.chunksize = BufferNeeds.bestFactor(tx.availableBuffs(), filesize);
        beforeFirst();
    }

    /**
     * Positions the scan before the first record.
     * 
     * @see simpledb.query.Scan#beforeFirst()
     */
    public void beforeFirst() {
        if (outer != null) {
            outer.close();
        }
        nextblknum = 0;
        int end = nextblknum + chunksize - 1;
        if (end >= filesize)
            end = filesize - 1;
        outer = new ChunkScan(tx, filename, outerlayout, nextblknum, end);
        nextblknum = end + 1;
        first = true;
    }

    /**
     * Moves to the next record in the current scan.
     * If there are no more records in the current chunk,
     * then move to the next LHS record and the beginning of the current chunk.
     * If there are no more LHS records, then move to the next chunk
     * and begin from the start of the LHS again.
     * 
     * If the current pair of records do not satisfy the join condition,
     * repeat until a pair is found.
     * 
     * @see simpledb.query.Scan#next()
     */
    public boolean next() {
        if (first && !useNextChunk()) {
            return false;
        } else if (first) {
            first = false;
        }
        while (true) {
            if (!outer.next()) {
                if (inner.next()) {
                    outer.beforeFirst();
                    outer.next();
                } else if (useNextChunk()) {
                    outer.next();
                } else {
                    return false;
                }
            }
            if (term.isSatisfied(outer, inner)) {
                return true;
            }
        }
    }

    /**
     * Returns the integer value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     * 
     * @see simpledb.query.Scan#getInt(java.lang.String)
     */
    public int getInt(String fldname) {
        if (inner.hasField(fldname)) {
            return inner.getInt(fldname);
        }
        return outer.getInt(fldname);
    }

    /**
     * Returns the value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     * 
     * @see simpledb.query.Scan#getVal(java.lang.String)
     */
    public Constant getVal(String fldname) {
        if (inner.hasField(fldname)) {
            return inner.getVal(fldname);
        }
        return outer.getVal(fldname);
    }

    /**
     * Returns the string value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     * 
     * @see simpledb.query.Scan#getString(java.lang.String)
     */
    public String getString(String fldname) {
        if (inner.hasField(fldname)) {
            return inner.getString(fldname);
        }
        return outer.getString(fldname);
    }

    /**
     * Returns true if the specified field is in
     * either of the underlying scans.
     * 
     * @see simpledb.query.Scan#hasField(java.lang.String)
     */
    public boolean hasField(String fldname) {
        return inner.hasField(fldname) || outer.hasField(fldname);
    }

    /**
     * Closes the current scans.
     * 
     * @see simpledb.query.Scan#close()
     */
    public void close() {
        inner.close();
        outer.close();
    }

    private boolean useNextChunk() {
        // For the first load, we do not need to reload the outer table.
        if (first) {
            first = false;
        } else {
            if (nextblknum >= filesize)
                return false;
            if (outer != null)
                outer.close();
            int end = nextblknum + chunksize - 1;
            if (end >= filesize)
                end = filesize - 1;
            outer = new ChunkScan(tx, filename, outerlayout, nextblknum, end);
            nextblknum = end + 1;
        }
        inner.beforeFirst();
        if (!inner.next()) {
            return false;
        }
        return true;
    }
}
