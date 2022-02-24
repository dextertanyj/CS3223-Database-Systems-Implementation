package simpledb.join;

import java.util.ArrayList;

import simpledb.index.hash.HashIndex;
import simpledb.materialize.TempTable;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.record.TableScan;

public class HashJoinScan implements Scan {
  private TableScan lhs, rhs;
  private HashIndex lhsIdx, rhsIdx;
  private String joinfield;
  private HashJoinTable hashTable;
  private int currPartitionIdx = 0;

  public HashJoinScan(
    TableScan lhs,
    TableScan rhs,
    HashIndex lhsIdx,
    HashIndex rhsIdx,
    String joinfield,
    ArrayList<TempTable> tempTables
  ) {
    this.lhs = lhs;
    this.rhs = rhs;
    this.lhsIdx = lhsIdx;
    this.rhsIdx = rhsIdx;
    this.joinfield = joinfield;
    this.hashTable = new HashJoinTable(joinfield, tempTables);
    beforeFirst();
  }

  public void beforeFirst() {
    currPartitionIdx = 0;
    lhs.beforeFirst();
    rhs.beforeFirst();
    fillHashTable(0);
    rhsIdx.beforeFirstBlock(0);
    hashTable.beforeFirst();
  }

  private void fillHashTable(int blockIdx) {
    hashTable.clear();
    lhsIdx.beforeFirstBlock(blockIdx);
    while (lhsIdx.nextRecord()) {
      lhs.moveToRid(lhsIdx.getDataRid());
      Constant val = lhs.getVal(joinfield);
      hashTable.insertConstant(val);
    }
    hashTable.beforeFirst();
  }

  public boolean next() {
    while(true) {
      if (!rhsIdx.nextRecord()) {
        currPartitionIdx++;
        if (currPartitionIdx > hashTable.partitionValue()) {
          return false;
        }
        rhsIdx.beforeFirstBlock(currPartitionIdx);
        fillHashTable(currPartitionIdx); 
      } else {
        rhs.moveToRid(rhsIdx.getDataRid());
        Constant val = rhs.getVal(joinfield);
        if (hashTable.search(val)) {
          return true;
        }
      }
    }
  }

  public int getInt(String fldname) {
    if (lhs.hasField(fldname)) {
      return lhs.getInt(fldname);
    }
    return rhs.getInt(fldname);
  }

  public Constant getVal(String fldname) {
    if (lhs.hasField(fldname)) {
      return lhs.getVal(fldname);
    }
    return rhs.getVal(fldname);
  }

  public String getString(String fldname) {
    if (lhs.hasField(fldname)) {
      return lhs.getString(fldname);
    }
    return rhs.getString(fldname);
  }

  public boolean hasField(String fldname) {
    return lhs.hasField(fldname) || rhs.hasField(fldname);
  }

  public void close() {
    hashTable.close();
    lhs.close();
    rhs.close();
    lhsIdx.close();
    rhsIdx.close();
  }
}
