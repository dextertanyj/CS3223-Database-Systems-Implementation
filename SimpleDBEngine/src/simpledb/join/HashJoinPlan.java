package simpledb.join;

import java.util.ArrayList;

import simpledb.index.hash.HashIndex;
import simpledb.materialize.TempTable;
import simpledb.metadata.IndexInfo;
import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

public class HashJoinPlan implements Plan {
   private Plan p1, p2;
   private IndexInfo ii1, ii2;
   private String joinfield;
   private Schema sch = new Schema();
   private Transaction tx;

   public HashJoinPlan(Plan p1, Plan p2, IndexInfo ii1, IndexInfo ii2, String joinfield, Transaction tx) {
      this.p1 = p1;
      this.p2 = p2;
      this.ii1 = ii1;
      this.ii2 = ii2;
      this.joinfield = joinfield;
      this.tx = tx;
      sch.addAll(p1.schema());
      sch.addAll(p2.schema());
   }

   private ArrayList<TempTable> initHashTable(int partitionValue) {
      ArrayList<TempTable> hashTable = new ArrayList<>();
      Schema hashSchema = new Schema();
      hashSchema.add(joinfield, sch);

      for (int i = 0; i < partitionValue; i++) {
         hashTable.add(new TempTable(tx, hashSchema));
      }
      return hashTable;
   } 

   public Scan open() {
      TableScan ts1 = (TableScan) p1.open();
      TableScan ts2 = (TableScan) p2.open();
      HashIndex idx1 = (HashIndex) ii1.open();
      HashIndex idx2 = (HashIndex) ii2.open();
      int partitionValue = tx.availableBuffs();
      return new HashJoinScan(ts1, ts2, idx1, idx2, joinfield, initHashTable(partitionValue));
   }

   public int blocksAccessed() {
      return p1.blocksAccessed() + p2.blocksAccessed() 
            + ii1.blocksAccessed() + ii2.blocksAccessed()
            + recordsOutput();
   }

   public int recordsOutput() {
      return Math.max(p1.recordsOutput(), p2.recordsOutput());
   }

   public int distinctValues(String fldname) {
      if (p1.schema().hasField(fldname))
         return p1.distinctValues(fldname);
      else
         return p2.distinctValues(fldname);
   }

   public Schema schema() {
      return sch;
   }
}
