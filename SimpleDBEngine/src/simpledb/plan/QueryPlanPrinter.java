package simpledb.plan;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Aggregates and prints the different type of plans for each query
 */
public class QueryPlanPrinter {
  private ArrayList<String> list = new ArrayList<>();
  private static final String pointerDown = "    " + "\u2191";
  private static final char joinDelimiter = '-';
  private static final char normDelimiter = ' ';

  /**
   * Constructor method that takes in only 1 line to be printed.
   * 
   * @param firstLine first line of query to be printed. This should be from TablePlan.
   */
  public QueryPlanPrinter(String firstLine) {
    this.list.add(firstLine);
    this.list.add(pointerDown);
  }

  private QueryPlanPrinter(ArrayList<String> list) {
    this.list = list;
  }

  /**
   * Joins the query plan strings for join queries.
   * 
   * @param lhs the lhs query plan
   * @param rhs the rhs query plan
   * @param delimiter the character to separate the two query plan strings
   * @return the joined query plan string
   */
  private static String getJoinDelimiter(String lhs, String rhs, char delimiter, final int position) {
    int freq = position - lhs.length();
    char[] chars = new char[freq];
    Arrays.fill(chars, delimiter);
    if (delimiter == joinDelimiter) {
      chars[0] = ' '; chars[1] = '<'; chars[chars.length - 1] = ' ';
    }
    return lhs + new String(chars) + rhs;
  }

  /**
   * Get the delimiter length required to prevent overlapping
   * @param larger the query plan strings to be joined onto
   * 
   * @return the length of the delimiter
   */
  private static int getDelimiterLength(ArrayList<String> larger) {
    int position = 5;
    int maxLength = 0;
    for (String query : larger) {
      if (query.length() > maxLength) {
        maxLength = query.length(); 
      }
    }
    while (position - maxLength < 5) {
      position += 5;
    }
    return position;
  }

  /**
   * Adds a query plan to be printed.
   * Appends an arrow at the next line of the string.
   * 
   * @param line query plan to tbe printed
   * @return this QueryPlanPrinter instance
   */
  public QueryPlanPrinter add(String line) {
    list.add(line);
    list.add(pointerDown);
    return this;
  }

  /**
   * Removes a query plan from the instance.
   * Removes the arrow for this query plan as well.
   * 
   * @return this QueryPlanPrinter instance
   */
  public QueryPlanPrinter remove() {
    list.remove(list.size() - 1);
    list.remove(list.size() - 1);
    return this;
  }

  /**
   * Creates a join plan description string.
   * 
   * @param joinPlan the name of the join plan.
   * @param fieldname1 the fieldname of the left table to perform the join on.
   * @param fieldname2 the fieldname of the right table to perform the join on.
   * @return the join plan description string.
   */
  public static String getJoinPlanDesc(String joinPlan, String fieldname1, String fieldname2) {
    return String.format("%s on: [%s, %s]", joinPlan, fieldname1, fieldname2);
  }

  /**
   * Combines two QueryPlanPrinter instances into one for join plans.
   * 
   * @param lhs the QueryPlanPrinter of the table on the left hand side.
   * @param rhs the QueryPlanPrinter of the table on the right hand side.
   * @return the newly combined QueryPlanPrinter.
   */
  public static QueryPlanPrinter getJoinPlanPrinter(QueryPlanPrinter lhs, QueryPlanPrinter rhs) {
    ArrayList<String> smaller;
    ArrayList<String> larger;
    if (lhs.list.size() < rhs.list.size()) {
      smaller = lhs.list; larger = rhs.list;
    } else {
      smaller = rhs.list; larger = lhs.list;
    }

    int diff = larger.size() - smaller.size();
    ArrayList<String> combined = new ArrayList<>();
    final int position = getDelimiterLength(larger);
    for (int i = 0; i < larger.size() - 1; ++i) {
      if (diff - i > 0) {
        combined.add(larger.get(i));
      } else if (i == larger.size() - 2) {
        combined.add(getJoinDelimiter(larger.get(i), smaller.get(i - diff), joinDelimiter, position));
      } else {
        combined.add(getJoinDelimiter(larger.get(i), smaller.get(i - diff), normDelimiter, position));
      }
    }
    combined.add(QueryPlanPrinter.pointerDown);
    return new QueryPlanPrinter(combined);
  }

  /**
   * Prints the QueryPlanPrinter as a string.
   * @return
   */
  public String printQueryPlan() {
    String output = "\n";
    for (int i = list.size() - 2; i >= 0; --i) {
      output += list.get(i) + "\n";
    }
    return output;
  }
}
