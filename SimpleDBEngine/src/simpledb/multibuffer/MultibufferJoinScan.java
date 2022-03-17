package simpledb.multibuffer;

import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.Term;
import simpledb.record.Layout;
import simpledb.tx.Transaction;

public class MultibufferJoinScan implements Scan {
    private Transaction tx;
    private String filename;
    private Layout outerlayout;
    private Scan outer, inner;
    private Term term;
    private int chunksize, nextblknum, filesize;
    private boolean first = true;

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

    public int getInt(String fldname) {
        if (inner.hasField(fldname)) {
            return inner.getInt(fldname);
        }
        return outer.getInt(fldname);
    }

    public Constant getVal(String fldname) {
        if (inner.hasField(fldname)) {
            return inner.getVal(fldname);
        }
        return outer.getVal(fldname);
    }

    public String getString(String fldname) {
        if (inner.hasField(fldname)) {
            return inner.getString(fldname);
        }
        return outer.getString(fldname);
    }

    public boolean hasField(String fldname) {
        return inner.hasField(fldname) || outer.hasField(fldname);
    }

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
