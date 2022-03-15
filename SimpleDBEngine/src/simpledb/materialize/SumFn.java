package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

/**
 * The <i>sum</i> aggregation function.
 */
public class SumFn implements AggregationFn {
   private String fldname;
   private int sum;

   /**
    * Create a sum aggregation function for the specified field
    *
    * @param fldname the name of the aggregated field
    */
   public SumFn(String fldname) {
      this.fldname = fldname;
   }

   /**
    * Starts tracking the sum of the field.
    */
   public void processFirst(Scan s) {
      sum = s.getInt(fldname);
   }

   /**
    * Update and compute the new value of sum
    */
   public void processNext(Scan s) {
      sum += s.getInt(fldname);
   }

   /**
    * Return the field's name, prepended by "sumof".
    */
   public String fieldName() {
      return "sumof" + fldname;
   }

   /**
    * Return the field's name.
    */
   public String sourceField() {
      return fldname;
   }

   /**
    * Return the current sum.
    */
   public Constant value() {
      return new Constant(sum);
   }
}
