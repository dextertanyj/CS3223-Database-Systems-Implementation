package test.integration;

import simpledb.server.SimpleDB;

public class SelectIndexTest {
  public static void main(String[] args) {
    TestUtils.setup("studentdbtest");
    try {
      SimpleDB db = new SimpleDB("studentdbtest");

      String qry = "select SName, DName, GRADE from STUDENT, DEPT, ENROLL where majorid = did and studentid = sid";
      TestUtils.doQuery(db.planner(), db.newTx(), qry);
    } catch (Exception e) {
        e.printStackTrace();
    }
    TestUtils.teardown("studentdbtest");
  }
}
