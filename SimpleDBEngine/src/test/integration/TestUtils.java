package test.integration;

import static java.sql.Types.INTEGER;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;

import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import test.system.TestData;

public class TestUtils {
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

            stmt = "create index majorid on STUDENT(MajorId) using btree";
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

            stmt = "create table EMPTY(Id int)";
            planner.executeUpdate(stmt, tx);
            tx.commit();
            tx = db.newTx();
            System.out.println("Table EMPTY created.");

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

    public static void systemSetup(String path) {
        SimpleDB db = new SimpleDB(path);
        
        Transaction tx = db.newTx();
        Planner planner = db.planner();
        
        // Initialise Student table
        String stmt = "create table student(SId int, SName varchar(10), MajorId int, GradYear int)";
        planner.executeUpdate(stmt, tx);
        tx.commit();
        tx = db.newTx();
        
        stmt = "create index studentid on student(SId) using hash";
        planner.executeUpdate(stmt, tx);
        tx.commit();
        tx = db.newTx();
        
        stmt = "create index majorid on student(MajorId) using btree";
        planner.executeUpdate(stmt, tx);
        tx.commit();
        tx = db.newTx();
        
        String[] studentData = TestData.studVals;
        stmt = "insert into student(SId, SName, MajorId, GradYear) values ";
        for (String s : studentData) {
            planner.executeUpdate(stmt + s, tx);
            tx.commit();
            tx = db.newTx();
        }
        
        // Initalise Dept table
        stmt = "create table dept(DId int, DName varchar(8))";
        planner.executeUpdate(stmt, tx);
        tx.commit();
        tx = db.newTx();
        
        stmt = "insert into dept(DId, DName) values ";
        String[] deptVals = TestData.deptVals;
        for (String s : deptVals) {
            planner.executeUpdate(stmt + s, tx);
            tx.commit();
            tx = db.newTx();
        }
        
        // Initialise Course table
        stmt = "create table course(CId int, Title varchar(20), DeptId int)";
        planner.executeUpdate(stmt, tx);
        tx.commit();
        tx = db.newTx();
        
        stmt = "insert into course(CId, Title, DeptId) values ";
        String[] courseVals = TestData.courseVals;
        for (String s : courseVals) {
            planner.executeUpdate(stmt + s, tx);
            tx.commit();
            tx = db.newTx();
        }
        
        // Initialise Section table
        stmt = "create table section(SectId int, CourseId int, Prof varchar(8), YearOffered int)";
        planner.executeUpdate(stmt, tx);
        tx.commit();
        tx = db.newTx();
        
        stmt = "insert into section(SectId, CourseId, Prof, YearOffered) values ";
        String[] sectVals = TestData.sectionVals;
        for (String s : sectVals) {
            planner.executeUpdate(stmt + s, tx);
            tx.commit();
            tx = db.newTx();
        }
        
        // Initialise Enroll table
        stmt = "create table enroll(EId int, StudentId int, SectionId int, Grade varchar(2))";
        planner.executeUpdate(stmt, tx);
        tx.commit();
        tx = db.newTx();
        
        stmt = "create index studentid on enroll(StudentId) using hash";
        planner.executeUpdate(stmt, tx);
        tx.commit();
        tx = db.newTx();
        
        stmt = "insert into enroll(EId, StudentId, SectionId, Grade) values ";
        String[] enrollVals = TestData.enrollVals;
        for (String s : enrollVals) {
            planner.executeUpdate(stmt + s, tx);
            tx.commit();
            tx = db.newTx();
        }

        // Initialise empty table
        stmt = "create table empty(Id int)";
        planner.executeUpdate(stmt, tx);
        tx.commit();
        tx = db.newTx();
        
        tx.commit();
    }

    public static void doQuery(Planner planner, Transaction tx, String cmd) {
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

    public static void doUpdate(Planner planner, Transaction tx, String cmd) {
        try {
            int howmany = planner.executeUpdate(cmd, tx);
            System.out.println(howmany + " records processed");
            tx.commit();
        } catch (Exception e) {
            System.out.println("Exception: " + e.getMessage());
            tx.rollback();
        }
    }

    public static int getColumnDisplaySize(Schema schema, String fieldname) {
        int fieldtype = schema.type(fieldname);
        int fieldlength = (fieldtype == INTEGER) ? 6 : schema.length(fieldname);
        return Math.max(fieldname.length(), fieldlength) + 1;
    }
}
