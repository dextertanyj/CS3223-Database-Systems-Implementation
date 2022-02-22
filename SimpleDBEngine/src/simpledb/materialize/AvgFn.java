package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class AvgFn implements AggregationFn {
   private String fldname;
   private int sum;
   private int count;

   public AvgFn(String fldname) {
      this.fldname = fldname;
   }

   public void processFirst(Scan s) {
      count = 1;
      sum = s.getInt(fldname);
   }

   public void processNext(Scan s) {
      count++;
      sum += s.getInt(fldname);
   }

   public String fieldName() {
      return "avgof" + fldname;
   }

   public Constant value() {
      return new Constant(sum / count);
   }
}
