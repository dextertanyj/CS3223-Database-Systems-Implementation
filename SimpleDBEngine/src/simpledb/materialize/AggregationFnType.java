package simpledb.materialize;

/**
 * An enum that converts strings to aggregation Functions.
 */
public enum AggregationFnType {
  SUM("sum"),
  COUNT("count"),
  AVG("avg"),
  MIN("min"),
  MAX("max");

  private final String str;

  /**
   * The constructor method of the enum.
   * 
   * @param str the corresponding string value that would 
   *            invoke the construction of its associated aggregation function
   */
  private AggregationFnType(String str) {
      this.str = str;
  }

  public String toString() {
      return this.str;
  }

  /**
   * Creates a new aggregation function, based on the input string.
   * 
   * @param str the input string that determines the type of aggregation function to be created
   * @param fldname the fieldname to generate the aggregation function on
   * @return the corresponding aggregation function
   */
  public static AggregationFn createAggregationFn(String str, String fldname) {
    if (SUM.str.equals(str)) {
      return new AvgFn(fldname);
    } else if (COUNT.str.equals(str)) {
      return new CountFn(fldname);
    } else if (AVG.str.equals(str)) {
      return new AvgFn(fldname);
    } else if (MIN.str.equals(str)) {
      return new MinFn(fldname);
    } else if (MAX.str.equals(str)) {
      return new MaxFn(fldname);
    }
    throw new RuntimeException();
  }
}
