package simpledb.materialize;

import java.util.ArrayList;
import java.util.List;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

/**
 * The Plan class for the <i>sort</i> operator.
 * 
 * @author Edward Sciore
 */
public class SortPlan implements Plan {
   private Transaction tx;
   private Plan p;
   private Schema sch;
   private RecordComparator comp;

   /**
    * Create a sort plan for the specified query.
    * 
    * @param p          the plan for the underlying query
    * @param sortfields the fields to sort by
    * @param tx         the calling transaction
    */
   public SortPlan(Transaction tx, Plan p, List<SortClause> sortclauses) {
      this.tx = tx;
      this.p = p;
      sch = p.schema();
      comp = new RecordComparator(sortclauses);
   }

   /**
    * This method is where most of the action is.
    * Up to 2 sorted temporary tables are created,
    * and are passed into SortScan for final merging.
    * 
    * @see simpledb.plan.Plan#open()
    */
   public Scan open() {
      Scan src = p.open();
      List<TempTable> runs = splitIntoRuns(src);
      src.close();
      while (runs.size() > 2)
         runs = doAMergeIteration(runs);
      return new SortScan(runs, comp);
   }

   public Scan openComplete() {
      Scan src = p.open();
      List<TempTable> runs = splitIntoRuns(src);
      src.close();
      while (runs.size() > 1)
         runs = doAMergeIteration(runs);
      return new SortScan(runs, comp);
   }

   /**
    * Return the number of blocks in the sorted table,
    * which is the same as it would be in a
    * materialized table.
    * It does <i>not</i> include the one-time cost
    * of materializing and sorting the records.
    * 
    * @see simpledb.plan.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      int buffers = tx.availableBuffs();
      int run_count = p.blocksAccessed() / buffers;
      int iterations = (int) Math.ceil(Math.log(run_count) / Math.log(2)); // Runs are merged two at a time
      int sort_cost = 2 * p.blocksAccessed() * (1 + iterations);
      Plan mp = new MaterializePlan(tx, p); // not opened; just for analysis
      return mp.blocksAccessed() + sort_cost;
   }

   /**
    * Return the number of records in the sorted table,
    * which is the same as in the underlying query.
    * 
    * @see simpledb.plan.Plan#recordsOutput()
    */
   public int recordsOutput() {
      return p.recordsOutput();
   }

   /**
    * Return the number of distinct field values in
    * the sorted table, which is the same as in
    * the underlying query.
    * 
    * @see simpledb.plan.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      return p.distinctValues(fldname);
   }

   /**
    * Return the schema of the sorted table, which
    * is the same as in the underlying query.
    * 
    * @see simpledb.plan.Plan#schema()
    */
   public Schema schema() {
      return sch;
   }

   private List<TempTable> splitIntoRuns(Scan src) {
      List<TempTable> temps = new ArrayList<>();
      src.beforeFirst();
      if (!src.next())
         return temps;
      TempTable currenttemp = new TempTable(tx, sch);
      temps.add(currenttemp);
      UpdateScan currentscan = currenttemp.open();
      while (copy(src, currentscan))
         if (comp.compare(src, currentscan) < 0) {
            // start a new run
            currentscan.close();
            currenttemp = new TempTable(tx, sch);
            temps.add(currenttemp);
            currentscan = (UpdateScan) currenttemp.open();
         }
      currentscan.close();
      return temps;
   }

   private List<TempTable> doAMergeIteration(List<TempTable> runs) {
      List<TempTable> result = new ArrayList<>();
      while (runs.size() > 1) {
         result.add(mergeRuns(runs));
      }
      if (runs.size() == 1)
         result.add(runs.get(0));
      return result;
   }

   private TempTable mergeRuns(List<TempTable> runs) {
      TempTable result = new TempTable(tx, sch);
      UpdateScan dest = result.open();
      int tablecount = Math.min(runs.size(), tx.availableBuffs());

      List<Scan> scans = new ArrayList<>(tablecount);
      List<Boolean> hasmore = new ArrayList<>(tablecount);
      for (int i = 0; i < tablecount; i++) {
         TempTable tbl = runs.remove(0);
         Scan s = tbl.open();
         scans.add(s);
         hasmore.add(s.next());
      }

      while (hasmore.contains(true)) {
         int chosen = -1;
         for (int i = 0; i < tablecount; i++) {
            if (hasmore.get(i)) {
               if (chosen == -1) {
                  chosen = i;
               }
               if (chosen == i) {
                  continue;
               }
               if (comp.compare(scans.get(i), scans.get(chosen)) < 0) {
                  chosen = i;
               }
            }
         }
         hasmore.set(chosen, copy(scans.get(chosen), dest));
      }

      for (Scan s : scans) {
         s.close();
      }
      dest.close();
      return result;
   }

   private boolean copy(Scan src, UpdateScan dest) {
      dest.insert();
      for (String fldname : sch.fields())
         dest.setVal(fldname, src.getVal(fldname));
      return src.next();
   }
}
