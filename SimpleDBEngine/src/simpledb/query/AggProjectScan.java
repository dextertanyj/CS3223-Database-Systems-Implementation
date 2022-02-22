package simpledb.query;

import java.util.HashSet;
import java.util.List;

import simpledb.materialize.AggregationFn;

public class AggProjectScan implements Scan {
  private Scan s;
  private boolean isFirstProcessed;
  private HashSet<String> fieldnames;
  private List<AggregationFn> aggFns;

  public AggProjectScan(Scan s, List<AggregationFn> aggFns, HashSet<String> fieldnames) {
    this.s = s;
    this.aggFns = aggFns;
    this.isFirstProcessed = false;
    this.fieldnames = fieldnames;
  }

  public void beforeFirst() {
    s.beforeFirst();
  }

  public boolean next() {
    if (!s.next()) {
      return false;
    }
    if (!isFirstProcessed) {
      isFirstProcessed = true;
      for (AggregationFn fn : aggFns) {
        fn.processFirst(s);
      }
    } else {
      for (AggregationFn fn : aggFns) {
        fn.processNext(s);
      }
    }
    return true;
  }

  public int getInt(String fieldname) {
    if (hasField(fieldname))
      return s.getInt(fieldname);
    else
      throw new RuntimeException("field " + fieldname + " not found.");
  }

  public String getString(String fieldname) {
    if (hasField(fieldname))
      return s.getString(fieldname);
    else
      throw new RuntimeException("field " + fieldname + " not found.");
  }

  public Constant getVal(String fieldname) {
    if (hasField(fieldname))
      return s.getVal(fieldname);
    else
      throw new RuntimeException("field " + fieldname + " not found.");
  }

  public boolean hasField(String fieldname) {
    return fieldnames.contains(fieldname); 
  }

  public void close() {
    s.close();
  }
}
