package simpledb.plan;

import java.util.Arrays;
import java.util.List;

import simpledb.materialize.SortClause;
import simpledb.materialize.SortPlan;
import simpledb.query.ProjectScan;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

/**
 * The Plan class corresponding to the <i>project</i>
 * relational algebra operator.
 * 
 * @author Edward Sciore
 */
public class ProjectPlan implements Plan {
   private Plan p;
   private Schema schema = new Schema();
   private boolean isDistinct;

   /**
    * Creates a new project node in the query tree,
    * having the specified subquery and field list.
    * 
    * @param p         the subquery
    * @param fieldlist the list of fields
    */
   public ProjectPlan(Plan p, List<String> fieldlist) {
      this.p = p;
      this.isDistinct = false;
      for (String fldname : fieldlist)
         schema.add(fldname, p.schema());
   }

   /**
    * Overloaded constructor that creates a new project node in the query tree.
    * Allows for distinct projections to be specified.
    *
    * @param p          the subquery
    * @param fieldlist  the list of fields
    * @param isDistinct if the projection is distinct
    * @param tx         the calling transaction
    */
   public ProjectPlan(Plan p, List<String> fieldlist, boolean isDistinct, Transaction tx) {
      this.p = p;
      if (isDistinct) {
         this.p = new SortPlan(tx, p, SortClause.generateDefaultSort(fieldlist));
      }
      this.isDistinct = isDistinct;
      for (String fldname : fieldlist)
         schema.add(fldname, p.schema());
   }

   /**
    * Creates a project scan for this query.
    * 
    * @see simpledb.plan.Plan#open()
    */
   public Scan open() {
      Scan s = p.open();
      return new ProjectScan(s, schema.fields(), isDistinct);
   }

   /**
    * Estimates the number of block accesses in the projection,
    * which is the same as in the underlying query.
    * 
    * @see simpledb.plan.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      return p.blocksAccessed();
   }

   /**
    * Estimates the number of output records in the projection,
    * which is the same as in the underlying query.
    * 
    * @see simpledb.plan.Plan#recordsOutput()
    */
   public int recordsOutput() {
      return p.recordsOutput();
   }

   /**
    * Estimates the number of distinct field values
    * in the projection,
    * which is the same as in the underlying query.
    * 
    * @see simpledb.plan.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      return p.distinctValues(fldname);
   }

   /**
    * Returns the schema of the projection,
    * which is taken from the field list.
    * 
    * @see simpledb.plan.Plan#schema()
    */
   public Schema schema() {
      return schema;
   }

   public QueryPlanPrinter getPlanDesc() {
      String prefix = "Project";
      QueryPlanPrinter printer = p.getPlanDesc();
      if (isDistinct) {
         prefix += " distinct: ";
      } else {
         prefix += ": ";
      }
      return printer.add(prefix + Arrays.toString(schema.fields().toArray()));
   }
}
