package simpledb.opt;

import java.util.Map;

import simpledb.index.planner.IndexJoinPlan;
import simpledb.index.planner.IndexSelectPlan;
import simpledb.metadata.IndexInfo;
import simpledb.metadata.MetadataMgr;
import simpledb.materialize.MergeJoinPlan;
import simpledb.multibuffer.MultibufferJoinPlan;
import simpledb.multibuffer.MultibufferProductPlan;
import simpledb.plan.HashJoinPlan;
import simpledb.plan.Plan;
import simpledb.plan.SelectPlan;
import simpledb.plan.TablePlan;
import simpledb.query.Constant;
import simpledb.query.Expression;
import simpledb.query.Operator;
import simpledb.query.Predicate;
import simpledb.query.Term;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

/**
 * This class contains methods for planning a single table.
 * 
 * @author Edward Sciore
 */
class TablePlanner {
   private TablePlan myplan;
   private Predicate mypred;
   private Schema myschema;
   private Map<String, IndexInfo> indexes;
   private Transaction tx;

   /**
    * Creates a new table planner.
    * The specified predicate applies to the entire query.
    * The table planner is responsible for determining
    * which portion of the predicate is useful to the table,
    * and when indexes are useful.
    * 
    * @param tblname the name of the table
    * @param mypred  the query predicate
    * @param tx      the calling transaction
    */
   public TablePlanner(String tblname, Predicate mypred, Transaction tx, MetadataMgr mdm) {
      this.mypred = mypred;
      this.tx = tx;
      myplan = new TablePlan(tx, tblname, mdm);
      myschema = myplan.schema();
      indexes = mdm.getIndexInfo(tblname, tx);
   }

   /**
    * Constructs a select plan for the table.
    * The plan will use an indexselect, if possible.
    * 
    * @return a select plan for the table.
    */
   public Plan makeSelectPlan() {
      Plan p = makeIndexSelect();
      if (p == null)
         p = myplan;
      return addSelectPred(p);
   }

   /**
    * Constructs a join plan of the specified plan
    * and the table. The method considers a block based nested loop join,
    * a GRACE hash join, a sort-merge join and an index join.
    * It choose the least cost join and returns it.
    * The method returns a cartesian product if no join is applicable.
    * 
    * @param current the specified plan
    * @return a join plan of the plan and this table
    */
   public Plan makeJoinPlan(Plan current) {
      Schema currsch = current.schema();
      Predicate joinpred = mypred.joinSubPred(myschema, currsch);
      if (joinpred == null)
         return null;
      Plan p = null;
      Plan loop = makeLoopJoin(current, currsch);
      Plan index = makeIndexJoin(current, currsch);
      Plan hash = makeHashJoin(current, currsch);
      Plan merge = makeMergeJoin(current, currsch);
      if (loop == null && index == null && merge == null && hash == null) {
         p = makeProductJoin(current, currsch);
      } else {
         int loop_cost = loop == null ? Integer.MAX_VALUE : Integer.MAX_VALUE;
         int index_cost = index == null ? Integer.MAX_VALUE : 0;
         int merge_cost = merge == null ? Integer.MAX_VALUE : Integer.MAX_VALUE;
         int hash_cost = hash == null ? Integer.MAX_VALUE : Integer.MAX_VALUE;
         int cost = Math.min(loop_cost, Math.min(index_cost, Math.min(merge_cost, hash_cost)));
         if (cost == index_cost) {
            p = index;
         } else if (cost == hash_cost) {
            p = hash;
         } else if (cost == merge_cost) {
            p = merge;
         } else {
            p = loop;
         }
      }
      return p;
   }

   /**
    * Constructs a product plan of the specified plan and
    * this table.
    * 
    * @param current the specified plan
    * @return a product plan of the specified plan and this table
    */
   public Plan makeProductPlan(Plan current) {
      Plan p = addSelectPred(myplan);
      return new MultibufferProductPlan(tx, current, p);
   }

   private Plan makeIndexSelect() {
      for (String fldname : indexes.keySet()) {
         Constant val = mypred.equatesWithConstant(fldname);
         if (val != null) {
            IndexInfo ii = indexes.get(fldname);
            System.out.println("index on " + fldname + " used");
            return new IndexSelectPlan(myplan, ii, val);
         }
      }
      return null;
   }

   /**
    * Constructs a multibuffer nested loop join plan of the specified plan and this
    * table.
    * 
    * @param current the specified plan
    * @param currsch the schema of the specified plan
    * @return a multibuffer nested loop join plan of the specified plan and this
    *         table
    */
   private Plan makeLoopJoin(Plan current, Schema currsch) {
      for (String fldname : myschema.fields()) {
         String outerfield = mypred.equatesWithField(fldname);
         if (outerfield != null && currsch.hasField(outerfield)) {
            Plan p = new MultibufferJoinPlan(tx, myplan, current,
                  new Term(Operator.EQ, new Expression(fldname), new Expression(outerfield)));
            p = addSelectPred(p);
            return addJoinPred(p, currsch, new Term(Operator.EQ, new Expression(fldname), new Expression(outerfield)));
         }
      }
      Schema combined = new Schema();
      combined.addAll(myschema);
      combined.addAll(currsch);
      for (String fldname : myschema.fields()) {
         Term term = mypred.comparesWithField(fldname);
         if (term != null && term.appliesTo(combined)) {
            Plan p = new MultibufferJoinPlan(tx, myplan, current, term);
            p = addSelectPred(p);
            return addJoinPred(p, currsch, term);
         }
      }
      return null;
   }

   /**
    * Constructs a merge sort join plan of the specified plan and this table.
    * 
    * @param current the specified plan
    * @param currsch the schema of the specified plan
    * @return a merge sort join plan of the specified plan and this table
    */
   private Plan makeMergeJoin(Plan current, Schema currsch) {
      for (String fldname : myschema.fields()) {
         String outerfield = mypred.equatesWithField(fldname);
         if (outerfield != null && currsch.hasField(outerfield)) {
            Plan p = new MergeJoinPlan(tx, myplan, current, fldname, outerfield);
            p = addSelectPred(p);
            return addJoinPred(p, currsch, new Term(Operator.EQ, new Expression(fldname), new Expression(outerfield)));
         }
      }
      return null;
   }

   /**
    * Constructs a hash join plan of the specified plan and this
    * table.
    * 
    * @param current the specified plan
    * @param currsch the schema of the specified plan
    * @return a hash join plan of the specified plan and this table
    */
   private Plan makeHashJoin(Plan current, Schema currsch) {
      for (String fldname : myschema.fields()) {
         String outerfield = mypred.equatesWithField(fldname);
         if (outerfield != null && currsch.hasField(outerfield)) {
            Plan p = new HashJoinPlan(tx, myplan, current, fldname, outerfield);
            p = addSelectPred(p);
            return addJoinPred(p, currsch, new Term(Operator.EQ, new Expression(fldname), new Expression(outerfield)));
         }
      }
      return null;
   }

   /**
    * Constructs an index join plan of the specified plan and this
    * table.
    * 
    * @param current the specified plan
    * @param currsch the schema of the specified plan
    * @return a index join plan of the specified plan and this table
    */
   private Plan makeIndexJoin(Plan current, Schema currsch) {
      for (String fldname : indexes.keySet()) {
         String outerfield = mypred.equatesWithField(fldname);
         if (outerfield != null && currsch.hasField(outerfield)) {
            IndexInfo ii = indexes.get(fldname);
            Plan p = new IndexJoinPlan(current, myplan, ii, outerfield);
            p = addSelectPred(p);
            return addJoinPred(p, currsch, new Term(Operator.EQ, new Expression(fldname), new Expression(outerfield)));
         }
      }
      return null;
   }

   private Plan makeProductJoin(Plan current, Schema currsch) {
      Plan p = makeProductPlan(current);
      return addJoinPred(p, currsch);
   }

   private Plan addSelectPred(Plan p) {
      Predicate selectpred = mypred.selectSubPred(myschema);
      if (selectpred != null)
         return new SelectPlan(p, selectpred);
      else
         return p;
   }

   private Plan addJoinPred(Plan p, Schema currsch) {
      Predicate joinpred = mypred.joinSubPred(currsch, myschema);
      if (joinpred != null)
         return new SelectPlan(p, joinpred);
      else
         return p;
   }

   private Plan addJoinPred(Plan p, Schema currsch, Term except) {
      Predicate joinpred = mypred.joinSubPred(currsch, myschema);
      joinpred = joinpred.removeTerm(except);
      if (joinpred != null)
         return new SelectPlan(p, joinpred);
      else
         return p;
   }
}
