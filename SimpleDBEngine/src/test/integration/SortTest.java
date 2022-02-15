package test.integration;

import simpledb.server.SimpleDB;

public class SortTest {

    public static void main(String[] args) {
        TestUtils.setup("studentdbtest");

        try {
            SimpleDB db = new SimpleDB("studentdbtest");

            String qry = "select SName from DEPT, STUDENT where MajorId = DId";
            TestUtils.doQuery(db.planner(), db.newTx(), qry);

            qry = "select SName, DName from DEPT, STUDENT where MajorId = DId order by DName, SName desc";
            TestUtils.doQuery(db.planner(), db.newTx(), qry);
        } catch (Exception e) {
            e.printStackTrace();
        }

        TestUtils.teardown("studentdbtest");
    }
}
