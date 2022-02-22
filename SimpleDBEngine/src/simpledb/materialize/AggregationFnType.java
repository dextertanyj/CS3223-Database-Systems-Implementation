package simpledb.materialize;

public enum AggregationFnType {
  SUM("SUM"),
  COUNT("COUNT"),
  AVG("AVG"),
  MIN("MIN"),
  MAX("MAX");

  private final String str;

  private AggregationFnType(String str) {
      this.str = str;
  }

  public String toString() {
      return this.str;
  }

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
