package test.integration;

import static java.sql.Types.INTEGER;

import java.util.List;
import java.util.Scanner;

import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class SimpleIJTest {
   public static void main(String[] args) {
      TestUtils.setup("studentdbtest");
      try (Scanner sc = new Scanner(System.in)) {
         SimpleDB db = new SimpleDB("studentdbtest");

         Transaction tx = db.newTx();
         Planner planner = db.planner();

         System.out.print("\nSQL> ");
         while (sc.hasNextLine()) {
            // process one line of input
            String cmd = sc.nextLine().trim();
            if (cmd.startsWith("exit")) {
               break;
            } else if (cmd.startsWith("select")) {
               TestUtils.doQuery(planner, tx, cmd);
            } else {
               TestUtils.doUpdate(planner, tx, cmd);
            }
            System.out.print("\nSQL> ");
            tx = db.newTx();
         }
         tx.commit();
      } catch (Exception e) {
         e.printStackTrace();
      }
      TestUtils.teardown("studentdbtest");
   }
}