package simpledb.materialize;

import java.util.Comparator;
import java.util.List;

import simpledb.parse.SortField;
import simpledb.parse.SortField.SortOrder;
import simpledb.query.Constant;
import simpledb.query.Scan;

/**
 * A comparator for scans.
 * 
 * @author Edward Sciore
 */
public class RecordComparator implements Comparator<Scan> {
   private List<SortField> fields;

   /**
    * Create a comparator using the specified fields,
    * using the ordering implied by its iterator.
    * 
    * @param fields a list of field names
    */
   public RecordComparator(List<SortField> fields) {
      this.fields = fields;
   }

   private int compare(Constant val1, Constant val2, SortOrder ord) {
      int result = val1.compareTo(val2);
      if (SortOrder.Desc.equals(ord)) {
         return -1 * result;
      }
      return result;
   }

   /**
    * Compare the current records of the two specified scans.
    * The sort fields are considered in turn.
    * When a field is encountered for which the records have
    * different values, those values are used as the result
    * of the comparison.
    * If the two records have the same values for all
    * sort fields, then the method returns 0.
    * 
    * @param s1 the first scan
    * @param s2 the second scan
    * @return the result of comparing each scan's current record according to the
    *         field list
    */
   public int compare(Scan s1, Scan s2) {
      for (SortField qf : fields) {
         Constant val1 = s1.getVal(qf.getField());
         Constant val2 = s2.getVal(qf.getField());
         int result = compare(val1, val2, qf.getSortOrder());
         if (result != 0)
            return result;
      }
      return 0;
   }
}
