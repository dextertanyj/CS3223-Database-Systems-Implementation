package test.integration;

import simpledb.server.SimpleDB;

public class ChangeMajorTest {
   public static void main(String[] args) {
      TestUtils.setup("studentdbtest");
      try {
         SimpleDB db = new SimpleDB("studentdbtest");

         String qry = "select SName, DName from DEPT, STUDENT where MajorId = DId";
         TestUtils.doQuery(db.planner(), db.newTx(), qry);

         String cmd = "update STUDENT set MajorId=30 where SName = 'amy'";
         TestUtils.doUpdate(db.planner(), db.newTx(), cmd);

         TestUtils.doQuery(db.planner(), db.newTx(), qry);
         System.out.println("Amy is now a drama major.");
      } catch (Exception e) {
         e.printStackTrace();
      }

      StudentMajorTest.run("studentdbtest");

      TestUtils.teardown("studentdbtest");
   }
}
