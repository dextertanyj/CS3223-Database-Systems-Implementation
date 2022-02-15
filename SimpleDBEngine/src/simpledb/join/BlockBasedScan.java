package simpledb.join;

import static java.sql.Types.INTEGER;

import java.util.ArrayList;
import java.util.List;

import simpledb.file.BlockId;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.record.Layout;
import simpledb.record.RecordPage;
import simpledb.tx.Transaction;

public class BlockBasedScan implements Scan {
    private Transaction tx;
    private Layout layout;
    private List<RecordPage> pages;
    private int bufferlimit;
    private String filename;
    private int currentpage;
    private int currentslot;

    public BlockBasedScan(Transaction tx, String tblname, Layout layout) {
        this.tx = tx;
        this.layout = layout;
        this.bufferlimit = tx.availableBuffs() - 2;
        this.pages = new ArrayList<>(bufferlimit);
        filename = tblname + ".tbl";
        if (tx.size(filename) == 0) {
            moveToNewBlock();
        } else {
            moveToBlock(0);
        }
    }

    // Methods that implement Scan

    public void beforeFirst() {
        moveToBlock(0);
    }

    public boolean next() {
        currentslot = pages.get(currentpage).nextAfter(currentslot);
        while (currentslot < 0) {
            if (atLastBlock()) {
                return false;
            }
            if (currentpage < pages.size()) {
                currentpage++;
            } else {
                moveToBlock(pages.get(currentpage).block().number() + 1);
            }
            currentslot = pages.get(currentpage).nextAfter(currentslot);
        }
        return true;
    }

    public int getInt(String fldname) {
        return pages.get(currentpage).getInt(currentslot, fldname);
    }

    public String getString(String fldname) {
        return pages.get(currentpage).getString(currentslot, fldname);
    }

    public Constant getVal(String fldname) {
        if (layout.schema().type(fldname) == INTEGER)
            return new Constant(getInt(fldname));
        else
            return new Constant(getString(fldname));
    }

    public boolean hasField(String fldname) {
        return layout.schema().hasField(fldname);
    }

    public void close() {
        if (pages == null) {
            return;
        }
        for (RecordPage page : pages) {
            tx.unpin(page.block());
        }
        pages.clear();
    }

    // Private auxiliary methods

    private void moveToBlock(int blknum) {
        close();
        while (blknum != tx.size(filename) - 1) {
            BlockId blk = new BlockId(filename, blknum);
            pages.add(new RecordPage(tx, blk, layout));
            blknum++;
        }
        currentpage = 0;
        currentslot = -1;
    }

    private void moveToNewBlock() {
        close();
        BlockId blk = tx.append(filename);
        pages.add(new RecordPage(tx, blk, layout));
        pages.get(0).format();
        currentslot = -1;
        currentpage = 0;
    }

    private boolean atLastBlock() {
        return pages.get(currentpage).block().number() == tx.size(filename) - 1;
    }
}
