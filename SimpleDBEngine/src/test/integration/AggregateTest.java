package test.integration;
import simpledb.server.SimpleDB;

public class AggregateTest {
   public static void main(String[] args) {
      try {
         TestUtils.setup("studentdb");
         SimpleDB db = new SimpleDB("studentdb");
         String qry = "select count(sid) from student group by majorid";
         TestUtils.doQuery(db.planner(), db.newTx(), qry);
      } catch (Exception e) {
         e.printStackTrace();
      }
      TestUtils.teardown("studentdb");
   }
}
