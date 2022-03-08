package simpledb.parse;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import simpledb.materialize.SortClause;
import simpledb.query.Predicate;

/**
 * Data for the SQL <i>select</i> statement.
 * 
 * @author Edward Sciore
 */
public class QueryData {
   private List<String> fields;
   private Collection<String> tables;
   private Predicate pred;
   private List<SortClause> sortclauses;
   private boolean isDistinct = false;

   /**
    * Saves the field and table list and predicate and if a distinct projection is required.
    */
   public QueryData(List<String> fields, Collection<String> tables, Predicate pred, boolean isDistinct) {
      this(fields, tables, pred, null, isDistinct);
   }

   public QueryData(List<String> fields, Collection<String> tables, Predicate pred, List<SortClause> sortclauses, boolean isDistinct) {
      this.fields = fields;
      this.tables = tables;
      this.pred = pred;
      this.sortclauses = sortclauses;
      this.isDistinct = isDistinct;
   }

   /**
    * Returns the fields mentioned in the select clause.
    * 
    * @return a list of field names
    */
   public List<String> fields() {
      return fields;
   }

   /**
    * Returns the tables mentioned in the from clause.
    * 
    * @return a collection of table names
    */
   public Collection<String> tables() {
      return tables;
   }

   /**
    * Returns the predicate that describes which
    * records should be in the output table.
    * 
    * @return the query predicate
    */
   public Predicate pred() {
      return pred;
   }

   public List<SortClause> sortclauses() {
      return sortclauses;
   }

   public boolean isDistinct() {
      return isDistinct;
   }

   public String toString() {
      String result = "select ";
      if (isDistinct) {
         result += "distinct ";
      }
      for (String fldname : fields)
         result += fldname + ", ";
      result = result.substring(0, result.length() - 2); // remove final comma
      result += " from ";
      for (String tblname : tables)
         result += tblname + ", ";
      result = result.substring(0, result.length() - 2); // remove final comma
      String predstring = pred.toString();
      if (!predstring.equals(""))
         result += " where " + predstring;
      if (sortclauses != null) {
         result += " order by ";
         Iterator<SortClause> iter = sortclauses.iterator();
         result += iter.next().toString();
         while (iter.hasNext()) {
            result += ", " + iter.next().toString();
         }
      }
      return result;
   }
}
