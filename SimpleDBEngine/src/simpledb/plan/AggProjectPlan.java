package simpledb.plan;

import java.util.HashSet;
import java.util.List;

import simpledb.materialize.AggregationFn;
import simpledb.query.AggProjectScan;
import simpledb.query.Scan;
import simpledb.record.Schema;

public class AggProjectPlan implements Plan {
   private Plan p;
   private Schema schema = new Schema();
   private List<AggregationFn> aggFns;
   private HashSet<String> fieldnames;

   public AggProjectPlan(Plan p, List<AggregationFn> aggFns) {
      this.p = p;
      this.aggFns = aggFns;
      for (AggregationFn aggFn : aggFns) {
         fieldnames.add(aggFn.fieldName());
         schema.add(aggFn.fieldName(), p.schema());
      }
   }

   public Scan open() {
      Scan s = p.open();
      return new AggProjectScan(s, aggFns, fieldnames);
   }

   public int blocksAccessed() {
      return p.blocksAccessed();
   }

   public int recordsOutput() {
      return 1;
   }

   public int distinctValues(String fieldname) {
      return 1;
   }

   public Schema schema() {
      return schema;
   }
}
