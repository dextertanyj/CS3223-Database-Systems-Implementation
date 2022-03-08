package simpledb.query;

import java.util.List;

import simpledb.record.InMemoryRecord;

/**
 * The scan class corresponding to the <i>project</i> relational
 * algebra operator.
 * All methods except hasField delegate their work to the
 * underlying scan.
 * 
 * @author Edward Sciore
 */
public class ProjectScan implements Scan {
   private Scan s;
   private List<String> fieldlist;
   private final boolean isDistinct;
   private InMemoryRecord prevVals;

   /**
    * Create a project scan having the specified
    * underlying scan and field list.
    * 
    * @param s         the underlying scan
    * @param fieldlist the list of field names
    */
   public ProjectScan(Scan s, List<String> fieldlist) {
      this.s = s;
      this.fieldlist = fieldlist;
      this.isDistinct = false;
   }

   /**
    * Overloaded constructor that allows specification of is distinct projection.
    *
    * @param s the underlying scan
    * @param fieldlist the list of field names
    * @param isDistinct if the projection is distinct
    */
   public ProjectScan(Scan s, List<String> fieldlist, boolean isDistinct) {
      this.s = s;
      this.fieldlist = fieldlist;
      this.isDistinct = isDistinct;
      if (this.isDistinct) {
         this.prevVals = new InMemoryRecord(fieldlist);
         this.prevVals.setFieldlist();
      }
   }

   public void beforeFirst() {
      s.beforeFirst();
   }

   public boolean next() {
      if (!isDistinct) {
         return s.next();
      } else {
         return isDistinctNext();
      }
   }

   private boolean isDistinctNext() {
      InMemoryRecord currVals = new InMemoryRecord(fieldlist);
      currVals.setFieldlist();
      while (true) {
         if (!s.next()) {
            return false;
         }
         for (String field : fieldlist) {
            currVals.setVal(field, s.getVal(field));
         }
         if (!currVals.equals(prevVals)) {
            prevVals.putAll(currVals);
            return true;
         } 
      }
   }

   public int getInt(String fldname) {
      if (hasField(fldname))
         return s.getInt(fldname);
      else
         throw new RuntimeException("field " + fldname + " not found.");
   }

   public String getString(String fldname) {
      if (hasField(fldname))
         return s.getString(fldname);
      else
         throw new RuntimeException("field " + fldname + " not found.");
   }

   public Constant getVal(String fldname) {
      if (hasField(fldname))
         return s.getVal(fldname);
      else
         throw new RuntimeException("field " + fldname + " not found.");
   }

   public boolean hasField(String fldname) {
      return fieldlist.contains(fldname);
   }

   public void close() {
      s.close();
   }
}
