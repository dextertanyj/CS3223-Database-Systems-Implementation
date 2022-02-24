package simpledb.join;

import java.util.ArrayList;

import simpledb.materialize.TempTable;
import simpledb.query.Constant;
import simpledb.record.TableScan;

public class HashJoinTable {
  private String joinfield;
  private int partitionValue;
  private ArrayList<TableScan> hashTable;
  
  public HashJoinTable(String joinfield, ArrayList<TempTable> tempTables) {
    this.joinfield = joinfield;
    this.partitionValue = tempTables.size();
    for (int i = 0; i < tempTables.size(); i++) {
      this.hashTable.add((TableScan) tempTables.get(i).open());
    }
  }

  public void beforeFirst() {
    for (int i = 0; i < hashTable.size(); i++) {
      hashTable.get(i).beforeFirst();
    }
  }

  public int partitionValue() {
    return partitionValue;
  }

  // Search method also deletes the value from the table
  public boolean search(Constant val) {
    int idx = val.hashCode() % partitionValue;
    TableScan ts = hashTable.get(idx);
    ts.beforeFirst();
    while (ts.next()) {
      if (val.equals(ts.getVal(joinfield))) {
        ts.delete();
        ts.beforeFirst();
        return true;
      }
    }
    return false;
  }

  public void insertConstant(Constant val) {
    // TODO: handle overflow here?
    int idx = val.hashCode() % partitionValue;
    TableScan bucket = hashTable.get(idx);
    bucket.insert();
    bucket.setVal(joinfield, val); 
  }

  public void clear() {
    for (int i = 0; i < hashTable.size(); i++) {
      TableScan ts = hashTable.get(i);
      ts.beforeFirst();
      while (ts.next()) {
        ts.delete();
      }
      ts.beforeFirst();
    }
  }

  public void close() {
    clear();
    for (int i = 0; i < hashTable.size(); i++) {
      hashTable.get(i).close();
    }
  }
}
