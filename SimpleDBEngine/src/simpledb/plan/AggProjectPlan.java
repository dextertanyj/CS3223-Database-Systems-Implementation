package simpledb.plan;

import java.util.HashSet;
import java.util.List;

import simpledb.materialize.AggregationFn;
import simpledb.query.AggProjectScan;
import simpledb.query.Scan;
import simpledb.record.Schema;

/**
 * The Plan class corresponding to 
 * the projection of aggregated fields without
 * a group by clause.
 */
public class AggProjectPlan implements Plan {
   private Plan p;
   private Schema schema = new Schema();
   private List<AggregationFn> aggFns;
   private HashSet<String> fieldnames;

   /**
    * Creates a new aggregate project node in the query tree.
    * This node contains the specified aggregation functions
    * to be performed.
    *
    * @param p the subquery
    * @param aggFns the list of aggregation functions to be projected on
    */
   public AggProjectPlan(Plan p, List<AggregationFn> aggFns) {
      this.p = p;
      this.aggFns = aggFns;
      fieldnames = new HashSet<>();
      for (AggregationFn aggFn : aggFns) {
         fieldnames.add(aggFn.fieldName());
         schema.add(aggFn.fieldName(), p.schema());
      }
   }

   /**
    * Creates an aggregate project scan.
    */
   public Scan open() {
      Scan s = p.open();
      return new AggProjectScan(s, aggFns, fieldnames);
   }

   /**
    * Estimates the number of blocks accesses in the projection,
    * which is the same as in the underlying query.
    */
   public int blocksAccessed() {
      return p.blocksAccessed();
   }

   /**
    * Returns the number of output records in the projection.
    * Only 1 record will be returned as it is an aggregation.
    */
   public int recordsOutput() {
      return 1;
   }

   /**
    * Returns the number of distinct fields in the projection.
    * There will only be 1 distinct value as there is only 1 record output.
    */
   public int distinctValues(String fieldname) {
      return 1;
   }

   /**
    * Returns the schema of the projection,
    * which is taken from the field names in the aggregation functions.
    */
   public Schema schema() {
      return schema;
   }
}
