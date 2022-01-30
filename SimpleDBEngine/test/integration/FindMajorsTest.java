import java.util.Scanner;

import simpledb.server.SimpleDB;
import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.Scan;
import simpledb.tx.Transaction;

public class FindMajorsTest {
   public static void main(String[] args) {
      System.out.print("Enter a department name: ");
      Scanner sc = new Scanner(System.in);
      String major = sc.next();
      sc.close();

      try {
         SimpleDB db = new SimpleDB("studentdb");

         Transaction tx = db.newTx();
         Planner planner = db.planner();

         String qry = "select sname, gradyear "
               + "from student, dept "
               + "where did = majorid "
               + "and dname = '" + major + "'";
         Plan p = planner.createQueryPlan(qry, tx);

         Scan s = p.open();

         System.out.println("Here are the " + major + " majors");
         System.out.println("Name\tGradYear");
         while (s.next()) {
            String sname = s.getString("sname");
            int gradyear = s.getInt("gradyear");
            System.out.println(sname + "\t" + gradyear);
         }
         s.close();
         tx.commit();
      } catch (Exception e) {
         e.printStackTrace();
      }
   }
}
