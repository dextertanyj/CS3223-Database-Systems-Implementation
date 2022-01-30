package test.simpledb.parse;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import simpledb.parse.Parser;
import simpledb.query.Expression;
import simpledb.query.Operator;
import simpledb.query.Term;

public class ParserTest {

    @Test
    public void term_Success() {
        Parser parser = new Parser("expr = expr");
        Term term = parser.term();
        assertEquals(term, new Term(Operator.EQ, new Expression("expr"), new Expression("expr")));
    }
}
