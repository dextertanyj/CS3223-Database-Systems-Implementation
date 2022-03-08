package simpledb.multibuffer;

import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.record.Layout;
import simpledb.tx.Transaction;

public class MultibufferJoinScan implements Scan {
    private Transaction tx;
    private String filename;
    private Layout outerlayout;
    private Scan outer, inner;
    private String inner_field, outer_field;
    private int chunksize, nextblknum, filesize;

    public MultibufferJoinScan(Transaction tx, String tblname, Layout layout, Scan inner, String outer_field,
            String inner_field) {
        this.tx = tx;
        this.inner = inner;
        this.filename = tblname + ".tbl";
        this.outerlayout = layout;
        this.outer_field = outer_field;
        this.inner_field = inner_field;
        this.filesize = tx.size(filename);
        this.chunksize = BufferNeeds.bestFactor(tx.availableBuffs(), filesize);
        beforeFirst();
    }

    public void beforeFirst() {
        nextblknum = 0;
        useNextChunk();
    }

    public boolean next() {
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
            if (inner.getVal(inner_field).equals(outer.getVal(outer_field))) {
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
        if (nextblknum >= filesize)
            return false;
        if (outer != null)
            outer.close();
        int end = nextblknum + chunksize - 1;
        if (end >= filesize)
            end = filesize - 1;
        outer = new ChunkScan(tx, filename, outerlayout, nextblknum, end);
        inner.beforeFirst();
        inner.next();
        nextblknum = end + 1;
        return true;
    }
}
