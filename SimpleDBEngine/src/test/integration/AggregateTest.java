package test.integration;

import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class AggregateTest {
   public static void main(String[] args) {
      try {
         SimpleDB db = new SimpleDB("studentdb");
         
         Transaction tx = db.newTx();
         Planner planner = db.planner();
         
         String qry = "Select sname from student group by majorid";
         Plan p = planner.createQueryPlan(qry, tx);
         
         Scan s = p.open();
         
         System.out.println("Aggregation result:");
         while (s.next()) {
            String max1 = s.getString("sname");
            System.out.println("max" + "\t" + max1);
         }
         s.close();
         tx.commit();
      } catch (Exception e) {
         e.printStackTrace();
      }
   }
}
