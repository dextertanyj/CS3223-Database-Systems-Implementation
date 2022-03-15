package simpledb.plan;

import java.util.ArrayList;
import java.util.List;

import simpledb.materialize.AggregationFn;
import simpledb.parse.BadSyntaxException;
import simpledb.parse.CreateIndexData;
import simpledb.parse.CreateTableData;
import simpledb.parse.CreateViewData;
import simpledb.parse.DeleteData;
import simpledb.parse.InsertData;
import simpledb.parse.ModifyData;
import simpledb.parse.Parser;
import simpledb.parse.QueryData;
import simpledb.tx.Transaction;

/**
 * The object that executes SQL statements.
 * 
 * @author Edward Sciore
 */
public class Planner {
   private QueryPlanner qplanner;
   private UpdatePlanner uplanner;

   public Planner(QueryPlanner qplanner, UpdatePlanner uplanner) {
      this.qplanner = qplanner;
      this.uplanner = uplanner;
   }

   /**
    * Creates a plan for an SQL select statement, using the supplied planner.
    * 
    * @param qry the SQL query string
    * @param tx  the transaction
    * @return the scan corresponding to the query plan
    */
   public Plan createQueryPlan(String qry, Transaction tx) {
      Parser parser = new Parser(qry);
      QueryData data = parser.query();
      verifyQuery(data);
      return qplanner.createPlan(data, tx);
   }

   /**
    * Executes an SQL insert, delete, modify, or
    * create statement.
    * The method dispatches to the appropriate method of the
    * supplied update planner,
    * depending on what the parser returns.
    * 
    * @param cmd the SQL update string
    * @param tx  the transaction
    * @return an integer denoting the number of affected records
    */
   public int executeUpdate(String cmd, Transaction tx) {
      Parser parser = new Parser(cmd);
      Object data = parser.updateCmd();
      verifyUpdate(data);
      if (data instanceof InsertData)
         return uplanner.executeInsert((InsertData) data, tx);
      else if (data instanceof DeleteData)
         return uplanner.executeDelete((DeleteData) data, tx);
      else if (data instanceof ModifyData)
         return uplanner.executeModify((ModifyData) data, tx);
      else if (data instanceof CreateTableData)
         return uplanner.executeCreateTable((CreateTableData) data, tx);
      else if (data instanceof CreateViewData)
         return uplanner.executeCreateView((CreateViewData) data, tx);
      else if (data instanceof CreateIndexData)
         return uplanner.executeCreateIndex((CreateIndexData) data, tx);
      else
         return 0;
   }

   // SimpleDB does not verify queries, although it should.
   private void verifyQuery(QueryData data) {
      // fail if attribute in select list do not appear in group clause
      checkAggregate(data.fields(), data.aggFns(), data.groupFields());
   }

   // SimpleDB does not verify updates, although it should.
   private void verifyUpdate(Object data) {
   }

   /**
    * Throws BadSyntaxException error if the query uses group by clause inappropriately.
    * @param selectList the list of select fields from the query
    * @param aggregationFnList the list of aggregation functions from the query
    * @param groupList the list of group fields from the query
    */
   private static void checkAggregate(List<String> selectList, List<AggregationFn> aggregationFnList, List<String> groupList) {
      if (groupList.size() == 0) {
         if (aggregationFnList.size() > 0 && aggregationFnList.size() != selectList.size()) {
            throw new BadSyntaxException();
         } 
      }

      if (groupList.size() > 0) {
         List<String> fieldsNotInAggregateList = getFieldsNotInAggregateList(selectList, aggregationFnList);
         for (String s : fieldsNotInAggregateList) {
            if (!groupList.contains(s)) {
               throw new BadSyntaxException();
            }
         }
      }
   }

   /**
    * Get select fields that are not found in the aggregationFnList.
    * @param selectList the list of select fields from the query
    * @param aggregationFnList the list of aggregation functions from the query
    * @return a list of string containing fields from the selectList that do not appear in the aggregationFnList
    */
   private static List<String> getFieldsNotInAggregateList(List<String> selectList, List<AggregationFn> aggregationFnList) {
      List<String> resultList = new ArrayList<>();
      for (String selectField : selectList) {
         boolean fieldExists = false;
         for (AggregationFn aggregate : aggregationFnList) {
            if (aggregate.fieldName().equals(selectField)) {
               fieldExists = true;
               break;
            }
         }
         if (!fieldExists) {
            resultList.add(selectField);
         }
      }
      return resultList;
   }
}
