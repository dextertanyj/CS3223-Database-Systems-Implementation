package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

/**
 * The <i>min</i> aggregation function.
 */
public class MinFn implements AggregationFn {
   private String fldname;
   private Constant val;

   /**
    * Create a min aggregation function for the specified field.
    *
    * @param fldname name of the aggregate field
    */
   public MinFn(String fldname) {
      this.fldname = fldname;
   }

   /**
    * Starts a new minimum to be the field value in the current record
    */
   public void processFirst(Scan s) {
      val = s.getVal(fldname);
   }

   /**
    * Replace the current minimum by the field value
    * in the current record, if it is lower.
    */
   public void processNext(Scan s) {
      Constant newval = s.getVal(fldname);
      if (newval.compareTo(val) < 0)
         val = newval;
   }

   /**
    * Return the field's name, prepended by "minof"
    */
   public String fieldName() {
      return "minof" + fldname;
   }

   /**
    * Return the field's name.
    */
   public String sourceField() {
      return fldname;
   }

   /**
    * Return the current minimum
    */
   public Constant value() {
      return val;
   }
}
