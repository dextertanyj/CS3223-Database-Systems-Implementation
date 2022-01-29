package simpledb.query;

import java.util.Arrays;

public enum Operator {
    EQ(new String[] { "=" }),
    GTE(new String[] { ">=" }),
    GT(new String[] { ">" }),
    LTE(new String[] { "<=" }),
    LT(new String[] { "<" }),
    NEQ(new String[] { "!=", "<>" });

    private final String[] representations;

    public static Operator getOperator(String str) {
        for (Operator op : Operator.values()) {
            if (Arrays.asList(op.representations).contains(str)) {
                return op;
            }
        }
        throw new RuntimeException();
    }

    private Operator(String[] representations) {
        this.representations = representations;
    }

    public String toString() {
        return representations[0];
    }
}
