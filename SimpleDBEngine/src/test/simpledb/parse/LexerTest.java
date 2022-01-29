package test.simpledb.parse;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import simpledb.parse.*;
import simpledb.query.Operator;

public class LexerTest {

    @Test
    public void eatOpr_LTE_Success() {
        Lexer lex = new Lexer("<=");
        assertEquals(lex.eatOpr(), Operator.LTE);
    }

    @Test
    public void eatOpr_GTE_Success() {
        Lexer lex = new Lexer(">=");
        assertEquals(lex.eatOpr(), Operator.GTE);
    }

    @Test
    public void eatOpr_GT_Success() {
        Lexer lex = new Lexer(">");
        assertEquals(lex.eatOpr(), Operator.GT);
    }

    @Test
    public void eatOpr_LT_Success() {
        Lexer lex = new Lexer("<");
        assertEquals(lex.eatOpr(), Operator.LT);
    }

    @Test
    public void eatOpr_NEQ_Success() {
        Lexer lex = new Lexer("<>");
        assertEquals(lex.eatOpr(), Operator.NEQ);
        lex = new Lexer("!=");
        assertEquals(lex.eatOpr(), Operator.NEQ);
    }

    @Test
    public void eatOpr_EQ_Success() {
        Lexer lex = new Lexer("=");
        assertEquals(lex.eatOpr(), Operator.EQ);
    }

    @Test
    public void eatOpr_Invalid_Throws_Exception() {
        Lexer lex = new Lexer("!>");
        assertThrows(BadSyntaxException.class, () -> lex.eatOpr());
    }
}
