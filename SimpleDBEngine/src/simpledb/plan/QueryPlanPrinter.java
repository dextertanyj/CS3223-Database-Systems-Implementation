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

  public QueryPlanPrinter(String firstLine) {
    this.list.add(firstLine);
    this.list.add(pointerDown);
  }

  private QueryPlanPrinter(ArrayList<String> list) {
    this.list = list;
  }

  private static String getJoinDelimiter(String lhs, String rhs, char delimiter) {
    final int position = 35;
    int freq = position - lhs.length();
    char[] chars = new char[freq];
    Arrays.fill(chars, delimiter);
    if (delimiter == joinDelimiter) {
      chars[0] = ' '; chars[1] = '<'; chars[chars.length - 1] = ' ';
    }
    return lhs + new String(chars) + rhs;
  }

  public QueryPlanPrinter add(String line) {
    list.add(line);
    list.add(pointerDown);
    return this;
  }

  public QueryPlanPrinter remove() {
    list.remove(list.size() - 1);
    list.remove(list.size() - 1);
    return this;
  }

  public static String getJoinPlanDesc(String joinPlan, String fieldname1, String fieldname2) {
    return String.format("%s on: [%s, %s]", joinPlan, fieldname1, fieldname2);
  }

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
    for (int i = 0; i < larger.size() - 1; ++i) {
      if (diff - i > 0) {
        combined.add(larger.get(i));
      } else if (i == larger.size() - 2) {
        combined.add(getJoinDelimiter(larger.get(i), smaller.get(i - diff), joinDelimiter));
      } else {
        combined.add(getJoinDelimiter(larger.get(i), smaller.get(i - diff), normDelimiter));
      }
    }
    combined.add(QueryPlanPrinter.pointerDown);
    return new QueryPlanPrinter(combined);
  }

  public String printQueryPlan() {
    String output = "\n";
    for (int i = list.size() - 2; i >= 0; --i) {
      output += list.get(i) + "\n";
    }
    return output;
  }
}
