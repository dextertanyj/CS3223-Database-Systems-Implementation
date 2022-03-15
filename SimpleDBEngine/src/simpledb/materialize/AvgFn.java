package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

/**
 * The <i>average</i> aggregation function.
 */
public class AvgFn implements AggregationFn {
   private String fldname;
   private int sum;
   private int count;

   /**
    * Create an average aggregation function for the specified field
    *
    * @param fldname the name of the aggregated field
    */
   public AvgFn(String fldname) {
      this.fldname = fldname;
   }

   /**
    * Starts a count and tracks sum to compute the average.
    */
   public void processFirst(Scan s) {
      count = 1;
      sum = s.getInt(fldname);
   }

   /**
    * Increments count and compute the new value of sum.
    */
   public void processNext(Scan s) {
      count++;
      sum += s.getInt(fldname);
   }

   /**
    * Return the field's name, prepended by "avgof".
    */
   public String fieldName() {
      return "avgof" + fldname;
   }

   /**
    * Return the field's name.
    */
   public String sourceField() {
      return fldname;
   }

   /**
    * Return the current average.
    */
   public Constant value() {
      return new Constant(sum / count);
   }
}
