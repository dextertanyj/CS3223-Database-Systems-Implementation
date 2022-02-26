package simpledb.query;

import java.util.HashSet;
import java.util.List;

import simpledb.materialize.AggregationFn;

/**
 * The scan class corresponding to the projection of aggregation functions
 * without group by clauses.
 */
public class AggProjectScan implements Scan {
  private Scan s;
  private HashSet<String> fieldnames;
  private List<AggregationFn> aggFns;

  /**
   * Creates a aggregation project scan 
   * having the specified aggregation functions and fieldnames.
   * 
   * @param s the underlying scan
   * @param aggFns the aggregation functions to be projected on
   * @param fieldnames the set of fieldnames to be projected on
   */
  public AggProjectScan(Scan s, List<AggregationFn> aggFns, HashSet<String> fieldnames) {
    this.s = s;
    this.aggFns = aggFns;
    this.fieldnames = fieldnames;
  }

  public void beforeFirst() {
    s.beforeFirst();
  }

  public boolean next() {
    if (s.next()) {
      for (AggregationFn fn : aggFns) {
        fn.processFirst(s);
      }
    }
    while (s.next()) {
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
