package integration;

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
      try (Scanner sc = new Scanner(System.in)) {
         SimpleDB db = new SimpleDB("studentdb");

         Transaction tx = db.newTx();
         Planner planner = db.planner();

         System.out.print("\nSQL> ");
         while (sc.hasNextLine()) {
            // process one line of input
            String cmd = sc.nextLine().trim();
            if (cmd.startsWith("exit"))
               break;
            else if (cmd.startsWith("select"))
               doQuery(planner, tx, cmd);
            else
               doUpdate(planner, tx, cmd);
            System.out.print("\nSQL> ");
            tx = db.newTx();
         }
         tx.commit();
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   private static void doQuery(Planner planner, Transaction tx, String cmd) {
      Plan plan = planner.createQueryPlan(cmd, tx);
      try {
         Schema schema = plan.schema();
         List<String> columns = schema.fields();
         int columncount = columns.size();
         int totalwidth = 0;

         // print header
         for (int i = 0; i < columncount; i++) {
            String fldname = columns.get(i);
            int width = getColumnDisplaySize(schema, fldname);
            totalwidth += width;
            String fmt = "%" + width + "s";
            System.out.format(fmt, fldname);
         }
         System.out.println();
         for (int i = 0; i < totalwidth; i++)
            System.out.print("-");
         System.out.println();

         Scan s = plan.open();
         // print records
         while (s.next()) {
            for (int i = 0; i < columncount; i++) {
               String fldname = columns.get(i);
               int fldtype = schema.type(fldname);
               int width = getColumnDisplaySize(schema, fldname);
               String fmt = "%" + width;
               if (fldtype == INTEGER) {
                  int ival = s.getInt(fldname);
                  System.out.format(fmt + "d", ival);
               } else {
                  String sval = s.getString(fldname);
                  System.out.format(fmt + "s", sval);
               }
            }
            System.out.println();
         }
         s.close();
         tx.commit();
      } catch (Exception e) {
         System.out.println("Exception: " + e.getMessage());
         tx.rollback();
      }
   }

   private static void doUpdate(Planner planner, Transaction tx, String cmd) {
      try {
         int howmany = planner.executeUpdate(cmd, tx);
         System.out.println(howmany + " records processed");
         tx.commit();
      } catch (Exception e) {
         System.out.println("Exception: " + e.getMessage());
         tx.rollback();
      }
   }

   private static int getColumnDisplaySize(Schema schema, String fieldname) {
      int fieldtype = schema.type(fieldname);
      int fieldlength = (fieldtype == INTEGER) ? 6 : schema.length(fieldname);
      return Math.max(fieldname.length(), fieldlength) + 1;
   }
}