package simpledb.query;

import simpledb.plan.Plan;
import simpledb.record.Schema;

/**
 * A term is a comparison between two expressions.
 * 
 * @author Edward Sciore
 *
 */
public class Term {
   private Expression lhs, rhs;
   private Operator op;

   /**
    * Create a new term that compares two expressions
    * for equality.
    * 
    * @param lhs the LHS expression
    * @param rhs the RHS expression
    */
   public Term(Operator op, Expression lhs, Expression rhs) {
      this.op = op;
      this.lhs = lhs;
      this.rhs = rhs;
   }

   /**
    * Return true if both of the term's expressions
    * evaluate to the same constant,
    * with respect to the specified scan.
    * 
    * @param s the scan
    * @return true if both expressions have the same value in the scan
    */
   public boolean isSatisfied(Scan s) {
      Constant lhsval = lhs.evaluate(s);
      Constant rhsval = rhs.evaluate(s);
      switch (op) {
         case EQ:
            return lhsval.equals(rhsval);
         case NEQ:
            return !lhsval.equals(rhsval);
         case GTE:
            return lhsval.compareTo(rhsval) >= 0;
         case GT:
            return lhsval.compareTo(rhsval) > 0;
         case LTE:
            return lhsval.compareTo(rhsval) <= 0;
         case LT:
            return lhsval.compareTo(rhsval) < 0;
      }
      return false;
   }

   public boolean isSatisfied(Scan scan1, Scan scan2) {
      if (!lhs.isFieldName() || !rhs.isFieldName()) {
         throw new RuntimeException("Expected both expressions in term to be fields.");
      }
      Constant lhsval = null;
      Constant rhsval = null;
      if (scan1.hasField(lhs.asFieldName())) {
         lhsval = lhs.evaluate(scan1);
      } else if (scan2.hasField(lhs.asFieldName())) {
         lhsval = lhs.evaluate(scan2);
      } else {
         throw new RuntimeException("Field not found.");
      }

      if (scan1.hasField(rhs.asFieldName())) {
         rhsval = rhs.evaluate(scan1);
      } else if (scan2.hasField(rhs.asFieldName())) {
         rhsval = rhs.evaluate(scan2);
      } else {
         throw new RuntimeException("Field not found.");
      }
      switch (op) {
         case EQ:
            return lhsval.equals(rhsval);
         case NEQ:
            return !lhsval.equals(rhsval);
         case GTE:
            return lhsval.compareTo(rhsval) >= 0;
         case GT:
            return lhsval.compareTo(rhsval) > 0;
         case LTE:
            return lhsval.compareTo(rhsval) <= 0;
         case LT:
            return lhsval.compareTo(rhsval) < 0;
      }
      return false;
   }

   /**
    * Calculate the extent to which selecting on the term reduces
    * the number of records output by a query.
    * For example if the reduction factor is 2, then the
    * term cuts the size of the output in half.
    * 
    * @param p the query's plan
    * @return the integer reduction factor.
    */
   public int reductionFactor(Plan p) {
      String lhsName, rhsName;
      if (lhs.isFieldName() && rhs.isFieldName()) {
         lhsName = lhs.asFieldName();
         rhsName = rhs.asFieldName();
         return Math.max(p.distinctValues(lhsName),
               p.distinctValues(rhsName));
      }
      if (lhs.isFieldName()) {
         lhsName = lhs.asFieldName();
         return p.distinctValues(lhsName);
      }
      if (rhs.isFieldName()) {
         rhsName = rhs.asFieldName();
         return p.distinctValues(rhsName);
      }
      // otherwise, the term equates constants
      if (lhs.asConstant().equals(rhs.asConstant()))
         return 1;
      else
         return Integer.MAX_VALUE;
   }

   /**
    * Determine if this term is of the form "F=c"
    * where F is the specified field and c is some constant.
    * If so, the method returns that constant.
    * If not, the method returns null.
    * 
    * @param fldname the name of the field
    * @return either the constant or null
    */
   public Constant equatesWithConstant(String fldname) {
      if (lhs.isFieldName() &&
            lhs.asFieldName().equals(fldname) &&
            !rhs.isFieldName())
         return rhs.asConstant();
      else if (rhs.isFieldName() &&
            rhs.asFieldName().equals(fldname) &&
            !lhs.isFieldName())
         return lhs.asConstant();
      else
         return null;
   }

   /**
    * Determine if this term is of the form "F1=F2"
    * where F1 is the specified field and F2 is another field.
    * If so, the method returns the name of that field.
    * If not, the method returns null.
    * 
    * @param fldname the name of the field
    * @return either the name of the other field, or null
    */
   public String equatesWithField(String fldname) {
      if (!op.equals(Operator.EQ)) {
         return null;
      }
      if (lhs.isFieldName() &&
            lhs.asFieldName().equals(fldname) &&
            rhs.isFieldName())
         return rhs.asFieldName();
      else if (rhs.isFieldName() &&
            rhs.asFieldName().equals(fldname) &&
            lhs.isFieldName())
         return lhs.asFieldName();
      else
         return null;
   }

   /**
    * Determine if this term is of the form "F1 (relational operator) F2"
    * where F1 is the specified field and F2 is another field.
    * If so, the method returns the name of that field.
    * If not, the method returns null.
    * 
    * @param fldname the name of the field
    * @return either the name of the other field, or null
    */
   public String comparesWithField(String fldname) {
      if (lhs.isFieldName() &&
            lhs.asFieldName().equals(fldname) &&
            rhs.isFieldName())
         return rhs.asFieldName();
      else if (rhs.isFieldName() &&
            rhs.asFieldName().equals(fldname) &&
            lhs.isFieldName())
         return lhs.asFieldName();
      else
         return null;
   }

   /**
    * Return true if both of the term's expressions
    * apply to the specified schema.
    * 
    * @param sch the schema
    * @return true if both expressions apply to the schema
    */
   public boolean appliesTo(Schema sch) {
      return lhs.appliesTo(sch) && rhs.appliesTo(sch);
   }

   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (obj == null || !(obj instanceof Term)) {
         return false;
      }
      Term term = (Term) obj;
      return this.op.equals(term.op)
            && this.rhs.equals(term.rhs)
            && this.lhs.equals(term.lhs);
   }

   public String toString() {
      return lhs.toString() + op.toString() + rhs.toString();
   }
}
