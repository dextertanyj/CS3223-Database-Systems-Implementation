package test.integration;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import simpledb.plan.Planner;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class CreateStudentDBTest {
   public static void main(String[] args) {
      setup("studentdb");
   }

   public static void setup(String path) {
      System.out.println("Setting up...");
      try {
         SimpleDB db = new SimpleDB(path);

         Transaction tx = db.newTx();
         Planner planner = db.planner();

         String stmt = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)";
         planner.executeUpdate(stmt, tx);
         tx.commit();
         tx = db.newTx();
         System.out.println("Table STUDENT created.");

         stmt = "create index studentid on STUDENT(SId) using hash";
         planner.executeUpdate(stmt, tx);
         tx.commit();
         tx = db.newTx();
         System.out.println("Index studentid on STUDENT created.");

         stmt = "create index majorid on STUDENT(MajorId) using tree";
         planner.executeUpdate(stmt, tx);
         tx.commit();
         tx = db.newTx();
         System.out.println("Index majorid on STUDENT created.");

         stmt = "insert into STUDENT(SId, SName, MajorId, GradYear) values ";
         String[] studvals = { "(1, 'joe', 10, 2021)",
               "(2, 'amy', 20, 2020)",
               "(3, 'max', 10, 2022)",
               "(4, 'sue', 20, 2022)",
               "(5, 'bob', 30, 2020)",
               "(6, 'kim', 20, 2020)",
               "(7, 'art', 30, 2021)",
               "(8, 'pat', 20, 2019)",
               "(9, 'lee', 10, 2021)" };
         for (int i = 0; i < studvals.length; i++) {
            planner.executeUpdate(stmt + studvals[i], tx);
            tx.commit();
            tx = db.newTx();
         }
         System.out.println("STUDENT records inserted.");

         stmt = "create table DEPT(DId int, DName varchar(8))";
         planner.executeUpdate(stmt, tx);
         tx.commit();
         tx = db.newTx();
         System.out.println("Table DEPT created.");

         stmt = "insert into DEPT(DId, DName) values ";
         String[] deptvals = { "(10, 'compsci')",
               "(20, 'math')",
               "(30, 'drama')" };
         for (int i = 0; i < deptvals.length; i++) {
            planner.executeUpdate(stmt + deptvals[i], tx);
            tx.commit();
            tx = db.newTx();
         }
         System.out.println("DEPT records inserted.");

         stmt = "create table COURSE(CId int, Title varchar(20), DeptId int)";
         planner.executeUpdate(stmt, tx);
         tx.commit();
         tx = db.newTx();
         System.out.println("Table COURSE created.");

         stmt = "insert into COURSE(CId, Title, DeptId) values ";
         String[] coursevals = { "(12, 'db systems', 10)",
               "(22, 'compilers', 10)",
               "(32, 'calculus', 20)",
               "(42, 'algebra', 20)",
               "(52, 'acting', 30)",
               "(62, 'elocution', 30)" };
         for (int i = 0; i < coursevals.length; i++) {
            planner.executeUpdate(stmt + coursevals[i], tx);
            tx.commit();
            tx = db.newTx();
         }
         System.out.println("COURSE records inserted.");

         stmt = "create table SECTION(SectId int, CourseId int, Prof varchar(8), YearOffered int)";
         planner.executeUpdate(stmt, tx);
         tx.commit();
         tx = db.newTx();
         System.out.println("Table SECTION created.");

         stmt = "insert into SECTION(SectId, CourseId, Prof, YearOffered) values ";
         String[] sectvals = { "(13, 12, 'turing', 2018)",
               "(23, 12, 'turing', 2019)",
               "(33, 32, 'newton', 2019)",
               "(43, 32, 'einstein', 2017)",
               "(53, 62, 'brando', 2018)" };
         for (int i = 0; i < sectvals.length; i++) {
            planner.executeUpdate(stmt + sectvals[i], tx);
            tx.commit();
            tx = db.newTx();
         }
         System.out.println("SECTION records inserted.");

         stmt = "create table ENROLL(EId int, StudentId int, SectionId int, Grade varchar(2))";
         planner.executeUpdate(stmt, tx);
         tx.commit();
         tx = db.newTx();
         System.out.println("Table ENROLL created.");

         stmt = "create index studentid on ENROLL(StudentId) using hash";
         planner.executeUpdate(stmt, tx);
         tx.commit();
         tx = db.newTx();
         System.out.println("Index studentid on ENROLL created.");

         stmt = "insert into ENROLL(EId, StudentId, SectionId, Grade) values ";
         String[] enrollvals = { "(14, 1, 13, 'A')",
               "(24, 1, 43, 'C' )",
               "(34, 2, 43, 'B+')",
               "(44, 4, 33, 'B' )",
               "(54, 4, 53, 'A' )",
               "(64, 6, 53, 'A' )" };
         for (int i = 0; i < enrollvals.length; i++) {
            planner.executeUpdate(stmt + enrollvals[i], tx);
            tx.commit();
            tx = db.newTx();
         }
         System.out.println("ENROLL records inserted.");

         tx.commit();
      } catch (Exception e) {
         e.printStackTrace();
      }
      System.out.println("Setup complete.");
   }

   public static void teardown(String path) {
      System.out.println("Tearing down...");
      try {
         FileUtils.deleteDirectory(new File(path));
      } catch (IOException e) {
         // Ignore the error
      }
      System.out.println("Teardown complete.");
   }
}
