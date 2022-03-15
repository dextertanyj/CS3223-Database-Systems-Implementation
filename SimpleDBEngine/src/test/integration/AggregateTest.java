package test.integration;
import simpledb.server.SimpleDB;

public class AggregateTest {
   public static void main(String[] args) {
      try {
         TestUtils.setup("studentdb");
         SimpleDB db = new SimpleDB("studentdb");
         String qry = "select sname from student";
         TestUtils.doQuery(db.planner(), db.newTx(), qry);
      } catch (Exception e) {
         e.printStackTrace();
      }
      TestUtils.teardown("studentdb");
   }
}
