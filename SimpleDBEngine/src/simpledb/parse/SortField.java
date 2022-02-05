package simpledb.parse;

import java.util.ArrayList;
import java.util.List;

public class SortField {
  public enum SortOrder {
    Asc("Asc"), Desc("Desc");

    private final String identifier;

    SortOrder(String identifier) {
      this.identifier = identifier;
    }

    public String toString() {
      return identifier;
    }
  }
  
  private String field;
  private SortOrder sortOrder;

  public SortField(String field) {
    this.field = field;
    this.sortOrder = SortOrder.Asc;
  }

  public SortField(String field, SortOrder sortOrder) {
    this.field = field;
    this.sortOrder = sortOrder;
  }

  public void setOrder(SortOrder newOrder) {
    sortOrder = newOrder;
  }

  public String getField() {
    return field;
  }

  public SortOrder getSortOrder() {
    return sortOrder;
  }

  public static List<String> getFieldNames(List<SortField> fields) {
    List<String> names = new ArrayList<>();
    for (SortField qf : fields) {
      names.add(qf.field);
    }
    return names;
  }

  public static List<SortField> convertAscSortField(List<String> names) {
    List<SortField> fields = new ArrayList<>();
    for (String name : names) {
      fields.add(new SortField(name, SortOrder.Asc));
    }
    return fields;
  }
}
