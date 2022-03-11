package simpledb.parse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import simpledb.index.IndexType;
import simpledb.materialize.AggregationFn;
import simpledb.materialize.AggregationFnType;
import simpledb.materialize.SortClause;
import simpledb.materialize.SortOrder;
import simpledb.query.Constant;
import simpledb.query.Expression;
import simpledb.query.Operator;
import simpledb.query.Predicate;
import simpledb.query.Term;
import simpledb.record.Schema;

/**
 * The SimpleDB parser.
 * 
 * @author Edward Sciore
 */
public class Parser {
   private Lexer lex;

   public Parser(String s) {
      lex = new Lexer(s);
   }

   // Methods for parsing predicates, terms, expressions, constants, and fields

   public String field() {
      return lex.eatId();
   }

   /**
    * Consumes a select attribute or aggregation attribute in the select clause of the query.
    * @return a pair of string and aggregation function if a aggregation attribute exists, 
    * null for the aggregation function otherwise
    */
   private Pair<String, AggregationFn> selectField() {
      if (lex.matchKeyword(AggregationFnType.SUM.toString().toLowerCase())) {
         return selectFieldWithAggregate(AggregationFnType.SUM);
      } else if (lex.matchKeyword(AggregationFnType.COUNT.toString().toLowerCase())) {
         return selectFieldWithAggregate(AggregationFnType.COUNT);
      } else if (lex.matchKeyword(AggregationFnType.AVG.toString().toLowerCase())) {
         return selectFieldWithAggregate(AggregationFnType.AVG);
      } else if (lex.matchKeyword(AggregationFnType.MIN.toString().toLowerCase())) {
         return selectFieldWithAggregate(AggregationFnType.MIN);
      } else if (lex.matchKeyword(AggregationFnType.MAX.toString().toLowerCase())) {
         return selectFieldWithAggregate(AggregationFnType.MAX);
      }
      return new Pair<>(field(), null);
   }

   /**
    * Helper method to consume the aggregation keywords.
    * @param type AggregationFnType enum
    * @return a pair of select field name and the aggregate function
    */
   private Pair<String, AggregationFn> selectFieldWithAggregate(AggregationFnType type) {
      lex.eatKeyword(type.toString().toLowerCase());
      lex.eatDelim('(');
      String field = field();
      lex.eatDelim(')');
      AggregationFn agg = AggregationFnType.createAggregationFn(type.toString(), field);
      return new Pair<>(agg.fieldName(), agg);
   }

   public Constant constant() {
      if (lex.matchStringConstant())
         return new Constant(lex.eatStringConstant());
      else
         return new Constant(lex.eatIntConstant());
   }

   public Expression expression() {
      if (lex.matchId())
         return new Expression(field());
      else
         return new Expression(constant());
   }

   public Term term() {
      Expression lhs = expression();
      Operator op = lex.eatOpr();
      Expression rhs = expression();
      return new Term(op, lhs, rhs);
   }

   public Predicate predicate() {
      Predicate pred = new Predicate(term());
      if (lex.matchKeyword("and")) {
         lex.eatKeyword("and");
         pred.conjoinWith(predicate());
      }
      return pred;
   }

   /**
    * Parses the query.
    * @return a QueryData object that encompasses all information about the query
    */
   public QueryData query() {
      lex.eatKeyword("select");
      boolean isDistinct = false;
      if (lex.matchKeyword("distinct")) {
         lex.eatKeyword("distinct");
         isDistinct = true;
      }
      Pair<List<String>, List<AggregationFn>> pair = selectList();
      lex.eatKeyword("from");
      Collection<String> tables = tableList();
      Predicate pred = new Predicate();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }

      List<String> groupclauses = new ArrayList<>();
      if (lex.matchKeyword("group")) {
         lex.eatKeyword("group");
         if (lex.matchKeyword("by")) {
            lex.eatKeyword("by");
            groupclauses = groupList();
         }
      }

      List<SortClause> orderclauses = new ArrayList<>();
      if (lex.matchKeyword("order")) {
         lex.eatKeyword("order");
         lex.eatKeyword("by");
         orderclauses = orderList();
      }
      return new QueryData(pair.getFirst(), tables, pred, orderclauses, groupclauses, pair.getSecond(), isDistinct);
   }

   /**
    * Consumes all select fields including aggregated attributes.
    * @return a pair of list of strings and list of aggregation functions
    */
   private Pair<List<String>, List<AggregationFn>> selectList() {
      List<String> selectFieldList = new ArrayList<String>();
      List<AggregationFn> aggregateList = new ArrayList<>();
      Pair<List<String>, List<AggregationFn>> listPair = new Pair<>(selectFieldList, aggregateList);
      selectListHelper(listPair);
      return listPair;
   }

   private void selectListHelper(Pair<List<String>, List<AggregationFn>> listPair) {
      Pair<String, AggregationFn> pair = selectField();
      listPair.getFirst().add(pair.getFirst());
      if (!pair.isSecondEmpty()) {
         listPair.getSecond().add(pair.getSecond());
      }
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         selectListHelper(listPair);
      }
   }

   private Collection<String> tableList() {
      Collection<String> L = new ArrayList<String>();
      L.add(lex.eatId());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(tableList());
      }
      return L;
   }

   private List<SortClause> orderList() {
      List<SortClause> L = new ArrayList<SortClause>();
      String fieldname = field();
      if (lex.matchKeyword(SortOrder.DESCENDING.toString())) {
         lex.eatKeyword(SortOrder.DESCENDING.toString());
         L.add(new SortClause(fieldname, SortOrder.DESCENDING));
      } else {
         if (lex.matchKeyword(SortOrder.ASCENDING.toString())) {
            lex.eatKeyword(SortOrder.ASCENDING.toString());
         }
         L.add(new SortClause(fieldname, SortOrder.ASCENDING));
      }
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(orderList());
      }
      return L;
   }

   /**
    * consumes all the elements in the group by clause.
    * @return a list of field names of the group by attributes
    */
   private List<String> groupList() {
      List<String> L = new ArrayList<String>();
      String fieldname = field();
      L.add(fieldname);
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(groupList());
      }
      return L;
   }

   // Methods for parsing the various update commands

   public Object updateCmd() {
      if (lex.matchKeyword("insert"))
         return insert();
      else if (lex.matchKeyword("delete"))
         return delete();
      else if (lex.matchKeyword("update"))
         return modify();
      else
         return create();
   }

   private Object create() {
      lex.eatKeyword("create");
      if (lex.matchKeyword("table"))
         return createTable();
      else if (lex.matchKeyword("view"))
         return createView();
      else
         return createIndex();
   }

   // Method for parsing delete commands

   public DeleteData delete() {
      lex.eatKeyword("delete");
      lex.eatKeyword("from");
      String tblname = lex.eatId();
      Predicate pred = new Predicate();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }
      return new DeleteData(tblname, pred);
   }

   // Methods for parsing insert commands

   public InsertData insert() {
      lex.eatKeyword("insert");
      lex.eatKeyword("into");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      List<String> flds = fieldList();
      lex.eatDelim(')');
      lex.eatKeyword("values");
      lex.eatDelim('(');
      List<Constant> vals = constList();
      lex.eatDelim(')');
      return new InsertData(tblname, flds, vals);
   }

   private List<String> fieldList() {
      List<String> L = new ArrayList<String>();
      L.add(field());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(fieldList());
      }
      return L;
   }

   private List<Constant> constList() {
      List<Constant> L = new ArrayList<Constant>();
      L.add(constant());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(constList());
      }
      return L;
   }

   // Method for parsing modify commands

   public ModifyData modify() {
      lex.eatKeyword("update");
      String tblname = lex.eatId();
      lex.eatKeyword("set");
      String fldname = field();
      lex.eatDelim('=');
      Expression newval = expression();
      Predicate pred = new Predicate();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }
      return new ModifyData(tblname, fldname, newval, pred);
   }

   // Method for parsing create table commands

   public CreateTableData createTable() {
      lex.eatKeyword("table");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      Schema sch = fieldDefs();
      lex.eatDelim(')');
      return new CreateTableData(tblname, sch);
   }

   private Schema fieldDefs() {
      Schema schema = fieldDef();
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         Schema schema2 = fieldDefs();
         schema.addAll(schema2);
      }
      return schema;
   }

   private Schema fieldDef() {
      String fldname = field();
      return fieldType(fldname);
   }

   private Schema fieldType(String fldname) {
      Schema schema = new Schema();
      if (lex.matchKeyword("int")) {
         lex.eatKeyword("int");
         schema.addIntField(fldname);
      } else {
         lex.eatKeyword("varchar");
         lex.eatDelim('(');
         int strLen = lex.eatIntConstant();
         lex.eatDelim(')');
         schema.addStringField(fldname, strLen);
      }
      return schema;
   }

   // Method for parsing create view commands

   public CreateViewData createView() {
      lex.eatKeyword("view");
      String viewname = lex.eatId();
      lex.eatKeyword("as");
      QueryData qd = query();
      return new CreateViewData(viewname, qd);
   }

   // Method for parsing create index commands

   public CreateIndexData createIndex() {
      lex.eatKeyword("index");
      String idxname = lex.eatId();
      lex.eatKeyword("on");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      String fldname = field();
      lex.eatDelim(')');
      lex.eatKeyword("using");
      if (lex.matchKeyword(IndexType.HASH.toString())) {
         lex.eatKeyword(IndexType.HASH.toString());
         return new CreateIndexData(idxname, tblname, fldname, IndexType.HASH);
      }
      if (lex.matchKeyword(IndexType.TREE.toString())) {
         lex.eatKeyword(IndexType.TREE.toString());
         return new CreateIndexData(idxname, tblname, fldname, IndexType.TREE);
      }
      throw new BadSyntaxException();
   }
}
