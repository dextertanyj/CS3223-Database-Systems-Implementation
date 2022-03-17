package test.integration;

import simpledb.server.SimpleDB;

public class MergeJoinTest {

    public static void main(String[] args) {
        TestUtils.setup("studentdbtest");

        try {
            SimpleDB db = new SimpleDB("studentdbtest");

            String qry = "select Prof, Title from COURSE, SECTION where CId = CourseId";
            TestUtils.doQuery(db.planner(), db.newTx(), qry);
        } catch (Exception e) {
            e.printStackTrace();
        }

        TestUtils.teardown("studentdbtest");
    }
}
