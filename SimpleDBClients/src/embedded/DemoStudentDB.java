package embedded;

import java.sql.Statement;
import java.sql.Connection;
import java.sql.Driver;

import simpledb.jdbc.embedded.EmbeddedDriver;

public class DemoStudentDB {
  public static void main(String[] args) {
    System.out.println("Setting up...");
    Driver d = new EmbeddedDriver();
    String url = "jdbc:simpledb:studentdb";

    try (Connection conn = d.connect(url, null);
        Statement connStmt = conn.createStatement()) {

      String stmt = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)";
      connStmt.executeUpdate(stmt);
      System.out.println("Table STUDENT created.");

      stmt = "create index sidindex on STUDENT(SId) using hash";
      connStmt.executeUpdate(stmt);
      System.out.println("Index sidindex on STUDENT created.");

      stmt = "create index majoridindex on STUDENT(MajorId) using btree";
      connStmt.executeUpdate(stmt);
      System.out.println("Index majoridindex on STUDENT created.");

      stmt = "insert into STUDENT(SId, SName, MajorId, GradYear) values ";
      String[] studvals = { "(1, 'James', 10, 2022)",
          "(2, 'Mary', 12, 2022)",
          "(3, 'Robert', 12, 2021)",
          "(4, 'Patricia', 5, 2019)",
          "(5, 'John', 11, 2021)",
          "(6, 'Jennifer', 19, 2020)",
          "(7, 'Michael', 11, 2023)",
          "(8, 'Linda', 20, 2022)",
          "(9, 'David', 8, 2021)",
          "(10, 'Elizabeth', 18, 2020)",
          "(11, 'Joseph', 13, 2022)",
          "(12, 'Barbara', 7, 2019)",
          "(13, 'Charles', 18, 2022)",
          "(14, 'Susan', 14, 2021)",
          "(15, 'Daniel', 2, 2019)",
          "(16, 'Matthew', 20, 2019)",
          "(17, 'Anthony', 14, 2019)",
          "(18, 'Mark', 1, 2021)",
          "(19, 'Donald', 20, 2021)",
          "(20, 'Steven', 3, 2023)",
          "(21, 'Paul', 3, 2020)",
          "(22, 'Andrew', 6, 2022)",
          "(23, 'Joshua', 5, 2021)",
          "(24, 'Kenneth', 13, 2023)",
          "(25, 'Kevin', 13, 2021)",
          "(26, 'Brian', 6, 2022)",
          "(27, 'George', 15, 2022)",
          "(28, 'Edward', 10, 2023)",
          "(29, 'Ronald', 7, 2021)",
          "(30, 'Timothy', 17, 2023)",
          "(31, 'Jason', 5, 2023)",
          "(32, 'Jeffrey', 15, 2022)",
          "(33, 'Ryan', 12, 2021)",
          "(34, 'Jacob', 19, 2022)",
          "(35, 'Gary', 12, 2020)",
          "(36, 'Nicholas', 15, 2020)",
          "(37, 'Eric', 19, 2023)",
          "(38, 'Jonathan', 11, 2021)",
          "(39, 'Stephen', 2, 2022)",
          "(40, 'Larry', 19, 2022)",
          "(41, 'Justin', 12, 2019)",
          "(42, 'Scott', 4, 2023)",
          "(43, 'Brandon', 17, 2021)",
          "(44, 'Benjamin', 16, 2019)",
          "(45, 'Samuel', 6, 2020)",
          "(46, 'Gregory', 6, 2021)",
          "(47, 'Frank', 17, 2019)",
          "(48, 'Alexander', 5, 2022)",
          "(49, 'Raymond', 5, 2023)",
          "(50, 'Patrick', 5, 2023)",
          "(51, 'Jack', 5, 2022)",
          "(52, 'Dennis', 7, 2021)",
          "(53, 'Jerry', 1, 2022)",
          "(54, 'Tyler', 13, 2022)",
          "(55, 'Aaron', 14, 2023)",
          "(56, 'Jose', 5, 2023)",
          "(57, 'Adam', 3, 2020)",
          "(58, 'Henry', 18, 2023)",
          "(59, 'Nathan', 12, 2020)",
          "(60, 'Douglas', 13, 2022)",
          "(61, 'Zachary', 3, 2022)",
          "(62, 'Peter', 14, 2019)",
          "(63, 'Kyle', 17, 2019)",
          "(64, 'Walter', 6, 2020)",
          "(65, 'Ethan', 4, 2023)",
          "(66, 'Jeremy', 19, 2021)",
          "(67, 'Harold', 3, 2020)",
          "(68, 'Keith', 14, 2021)",
          "(69, 'Christian', 13, 2023)",
          "(70, 'Roger', 9, 2022)",
          "(71, 'Noah', 4, 2020)",
          "(72, 'Gerald', 9, 2019)",
          "(73, 'Carl', 12, 2021)",
          "(74, 'Terry', 18, 2023)",
          "(75, 'Sean', 10, 2023)",
      };
      for (int i = 0; i < studvals.length; i++) {
        connStmt.executeUpdate(stmt + studvals[i]);
      }
      System.out.println("STUDENT records inserted.");

      stmt = "create table DEPT(DId int, DName varchar(20))";
      connStmt.executeUpdate(stmt);
      System.out.println("Table DEPT created.");

      stmt = "insert into DEPT(DId, DName) values ";
      String[] deptvals = {
          "(1, 'Computer Science')",
          "(2, 'Information Systems')",
          "(3, 'Information Security')",
          "(4, 'Business Analytics')",
          "(5, 'Physics')",
          "(6, 'Chemistry')",
          "(7, 'Biology')",
          "(8, 'Mathematics')",
          "(9, 'Aero Engineering')",
          "(10, 'Mech Engineering')",
          "(11, 'Electric Engineering')",
          "(12, 'Civil Engineering')",
          "(13, 'Dentistry')",
          "(14, 'Law')",
          "(15, 'Medicine')",
          "(16, 'Accountancy')",
          "(17, 'Business')",
          "(18, 'Film')",
          "(19, 'Drama')",
          "(20, 'Art')", };
      for (int i = 0; i < deptvals.length; i++) {
        connStmt.executeUpdate(stmt + deptvals[i]);
      }
      System.out.println("DEPT records inserted.");

      stmt = "create table COURSE(CId int, Title varchar(30), DeptId int)";
      connStmt.executeUpdate(stmt);
      System.out.println("Table COURSE created.");

      stmt = "insert into COURSE(CId, Title, DeptId) values ";
      String[] coursevals = {
          "(1, 'Programming Methodology', 1)",
          "(2, 'Digital Media Marketing', 2)",
          "(3, 'Digital Forensics', 3)",
          "(4, 'Applied Econometrics II', 4)",
          "(5, 'Fundamentals of Physics', 5)",
          "(6, 'Fundamentals of Chemistry', 6)",
          "(7, 'General Biology', 7)",
          "(8, 'Calculus', 8)",
          "(9, 'Aerodynamics', 9)",
          "(10, 'Heat Transfer', 10)",
          "(11, 'Analytical Methods', 11)",
          "(12, 'Structural Engineering', 12)",
          "(13, 'Dental Implantology', 13)",
          "(14, 'Civil Law', 14)",
          "(15, 'Medicinal Chemistry', 15)",
          "(16, 'Corporate Law', 16)",
          "(17, 'Principles of Marketing', 17)",
          "(18, 'Philosophy and Film', 18)",
          "(19, 'Stylistics and Drama', 19)",
          "(20, 'Art in Society', 20)",
          "(21, 'Discrete Structures', 1)",
          "(22, 'Mobile Apps Development', 2)",
          "(23, 'Penetration Testing Practice', 3)",
          "(24, 'Econometric Models', 4)",
          "(25, 'Electricity & Magnetism I', 5)",
          "(26, 'Inorganic Chemistry 2', 6)",
          "(27, 'Animal Behaviour', 7)",
          "(28, 'Linear Algebra', 8)",
          "(29, 'Mechanics of Materials', 9)",
          "(30, 'Mechanics Of Machines', 10)",
          "(31, 'Electrical Engineering', 11)",
          "(32, 'Soil Mechanics', 12)",
          "(33, 'Paediatric Dentistry', 13)",
          "(34, 'Criminal Law', 14)",
          "(35, 'Pharmacy', 15)",
          "(36, 'Accounting Decision', 16)",
          "(37, 'Financial Markets', 17)",
          "(38, 'History of Film', 18)",
          "(39, 'Acting', 19)",
          "(40, 'Installation Art', 20)",
          "(41, 'Data Structures', 1)",
          "(42, 'Digital Product Management', 2)",
          "(43, 'Legal Aspects of Security', 3)",
          "(44, 'Innovation Theory', 4)",
          "(45, 'Quantum Mechanics', 5)",
          "(46, 'Physical Chemistry', 6)",
          "(47, 'Molecular Genetics', 7)",
          "(48, 'Differential Equations', 8)",
          "(49, 'Engineering Thermodynamics', 9)",
          "(50, 'Engineering Principles', 10)",
      };
      for (int i = 0; i < coursevals.length; i++) {
        connStmt.executeUpdate(stmt + coursevals[i]);
      }
      System.out.println("COURSE records inserted.");

      stmt = "create table SECTION(SectId int, CourseId int, Prof varchar(10), YearOffered int)";
      connStmt.executeUpdate(stmt);
      System.out.println("Table SECTION created.");

      stmt = "insert into SECTION(SectId, CourseId, Prof, YearOffered) values ";
      String[] sectvals = {
          "(1, 47, 'Khari', 2019)",
          "(2, 4, 'Azaria', 2018)",
          "(3, 31, 'Alisha', 2019)",
          "(4, 5, 'Marcellus', 2018)",
          "(5, 12, 'Benton', 2019)",
          "(6, 12, 'Justus', 2018)",
          "(7, 27, 'Kabir', 2019)",
          "(8, 6, 'Harlem', 2017)",
          "(9, 26, 'Harlem', 2019)",
          "(10, 13, 'Shepard', 2018)",
          "(11, 18, 'Marcellus', 2018)",
          "(12, 35, 'Amia', 2019)",
          "(13, 20, 'Terrell', 2019)",
          "(14, 48, 'Lian', 2018)",
          "(15, 46, 'Alaric', 2018)",
          "(16, 11, 'Gibson', 2019)",
          "(17, 36, 'Shepard', 2018)",
          "(18, 46, 'Korbyn', 2019)",
          "(19, 34, 'Violeta', 2018)",
          "(20, 39, 'Coleman', 2018)",
          "(21, 39, 'Florence', 2018)",
          "(22, 28, 'Zola', 2017)",
          "(23, 23, 'Ahmir', 2019)",
          "(24, 19, 'Ralph', 2019)",
          "(25, 5, 'Valery', 2017)",
          "(26, 19, 'Javion', 2017)",
          "(27, 18, 'Alaric', 2018)",
          "(28, 34, 'Janessa', 2019)",
          "(29, 34, 'Bellamy', 2018)",
          "(30, 13, 'Shepard', 2017)",
          "(31, 7, 'Marcellus', 2018)",
          "(32, 50, 'Terrence', 2019)",
          "(33, 15, 'Jenesis', 2018)",
          "(34, 39, 'Immanuel', 2017)",
          "(35, 29, 'Ahmir', 2019)",
          "(36, 41, 'Taliyah', 2019)",
          "(37, 35, 'Terrell', 2018)",
          "(38, 18, 'Maliah', 2017)",
          "(39, 42, 'Jenesis', 2017)",
          "(40, 9, 'Queen', 2019)",
          "(41, 15, 'Novah', 2019)",
          "(42, 11, 'Camdyn', 2018)",
          "(43, 36, 'Terrell', 2019)",
          "(44, 22, 'Amia', 2019)",
          "(45, 33, 'Randall', 2019)",
          "(46, 20, 'Terrence', 2019)",
          "(47, 29, 'Ernest', 2018)",
          "(48, 3, 'Genesis', 2018)",
          "(49, 38, 'Kabir', 2019)",
          "(50, 44, 'Howard', 2018)",
          "(51, 37, 'Kailyn', 2018)",
          "(52, 29, 'Cedric', 2017)",
          "(53, 9, 'Zora', 2018)",
          "(54, 2, 'Torin', 2017)",
          "(55, 50, 'Benedict', 2017)",
          "(56, 29, 'Bowie', 2017)",
          "(57, 22, 'Ingrid', 2017)",
          "(58, 46, 'Javion', 2019)",
          "(59, 45, 'Joselyn', 2017)",
          "(60, 22, 'Ezra', 2018)",
          "(61, 12, 'Creed', 2017)",
          "(62, 36, 'Paola', 2017)",
          "(63, 19, 'Harper', 2017)",
          "(64, 24, 'Hadleigh', 2018)",
          "(65, 39, 'Paola', 2018)",
          "(66, 25, 'Bellamy', 2019)",
          "(67, 11, 'Zavier', 2017)",
          "(68, 14, 'Markus', 2017)",
          "(69, 18, 'Robin', 2019)",
          "(70, 38, 'Arden', 2019)",
          "(71, 39, 'Kabir', 2017)",
          "(72, 11, 'Vada', 2017)",
          "(73, 8, 'Ellison', 2017)",
          "(74, 2, 'Austyn', 2018)",
          "(75, 19, 'Maliah', 2019)",
          "(76, 27, 'Randall', 2019)",
          "(77, 1, 'Forest', 2019)",
          "(78, 26, 'Etta', 2017)",
          "(79, 48, 'Austyn', 2018)",
          "(80, 7, 'Luella', 2017)",
          "(81, 41, 'Harper', 2017)",
          "(82, 4, 'Kylen', 2017)",
          "(83, 35, 'Randall', 2018)",
          "(84, 39, 'Lian', 2019)",
          "(85, 3, 'Jericho', 2018)",
          "(86, 38, 'Alaric', 2017)",
          "(87, 7, 'Austyn', 2017)",
          "(88, 16, 'Gus', 2017)",
          "(89, 5, 'Camilo', 2019)",
          "(90, 45, 'Ellison', 2017)",
          "(91, 31, 'Robin', 2017)",
          "(92, 22, 'Florence', 2018)",
          "(93, 18, 'Wendy', 2019)",
          "(94, 3, 'Ingrid', 2018)",
          "(95, 6, 'Ellison', 2019)",
          "(96, 9, 'Queen', 2017)",
          "(97, 10, 'Jayden', 2018)",
          "(98, 32, 'Jad', 2019)",
          "(99, 14, 'Decker', 2017)",
          "(100, 47, 'Markus', 2017)",
      };
      for (int i = 0; i < sectvals.length; i++) {
        connStmt.executeUpdate(stmt + sectvals[i]);
      }
      System.out.println("SECTION records inserted.");

      stmt = "create table ENROLL(EId int, StudentId int, SectionId int, Grade varchar(2))";
      connStmt.executeUpdate(stmt);
      System.out.println("Table ENROLL created.");

      stmt = "create index studentidindex on ENROLL(StudentId) using hash";
      connStmt.executeUpdate(stmt);
      System.out.println("Index studentidindex on ENROLL created.");

      stmt = "insert into ENROLL(EId, StudentId, SectionId, Grade) values ";
      String[] enrollvals = {
          "(1, 36, 39, 'D')",
          "(2, 12, 53, 'B')",
          "(3, 33, 58, 'B')",
          "(4, 71, 15, 'C+')",
          "(5, 46, 14, 'D+')",
          "(6, 54, 57, 'D')",
          "(7, 75, 27, 'A')",
          "(8, 53, 21, 'B')",
          "(9, 74, 84, 'C+')",
          "(10, 34, 34, 'D+')",
          "(11, 56, 47, 'A+')",
          "(12, 51, 90, 'B+')",
          "(13, 38, 18, 'A-')",
          "(14, 49, 16, 'A-')",
          "(15, 34, 25, 'D+')",
          "(16, 7, 24, 'F')",
          "(17, 32, 65, 'F')",
          "(18, 73, 24, 'B')",
          "(19, 75, 89, 'D+')",
          "(20, 23, 42, 'A+')",
          "(21, 41, 5, 'A')",
          "(22, 42, 88, 'C')",
          "(23, 28, 73, 'C')",
          "(24, 32, 38, 'C+')",
          "(25, 69, 50, 'B+')",
          "(26, 44, 75, 'F')",
          "(27, 71, 97, 'D')",
          "(28, 54, 33, 'B')",
          "(29, 69, 70, 'B+')",
          "(30, 25, 15, 'E')",
          "(31, 35, 8, 'E')",
          "(32, 47, 27, 'C+')",
          "(33, 11, 88, 'B')",
          "(34, 32, 4, 'A')",
          "(35, 72, 3, 'D')",
          "(36, 26, 38, 'B+')",
          "(37, 25, 31, 'D')",
          "(38, 19, 59, 'B-')",
          "(39, 48, 68, 'A')",
          "(40, 23, 15, 'E')",
          "(41, 42, 86, 'C+')",
          "(42, 56, 71, 'E')",
          "(43, 65, 97, 'F')",
          "(44, 43, 99, 'B-')",
          "(45, 64, 59, 'A-')",
          "(46, 3, 43, 'A')",
          "(47, 69, 98, 'D')",
          "(48, 20, 39, 'F')",
          "(49, 1, 31, 'B-')",
          "(50, 67, 46, 'A-')",
          "(51, 28, 95, 'A')",
          "(52, 41, 7, 'C')",
          "(53, 69, 99, 'C')",
          "(54, 39, 6, 'D+')",
          "(55, 32, 56, 'A')",
          "(56, 42, 2, 'B+')",
          "(57, 61, 71, 'C+')",
          "(58, 55, 69, 'F')",
          "(59, 71, 71, 'F')",
          "(60, 64, 98, 'B')",
          "(61, 43, 84, 'A')",
          "(62, 72, 68, 'A+')",
          "(63, 4, 29, 'C+')",
          "(64, 22, 47, 'B')",
          "(65, 18, 24, 'C+')",
          "(66, 2, 48, 'A')",
          "(67, 72, 38, 'B+')",
          "(68, 26, 13, 'B+')",
          "(69, 44, 94, 'C+')",
          "(70, 56, 31, 'D')",
          "(71, 73, 37, 'B')",
          "(72, 11, 92, 'B+')",
          "(73, 72, 34, 'A+')",
          "(74, 62, 49, 'B+')",
          "(75, 27, 6, 'B+')",
          "(76, 27, 6, 'A')",
          "(77, 53, 54, 'B-')",
          "(78, 71, 1, 'A+')",
          "(79, 21, 76, 'A+')",
          "(80, 59, 85, 'D+')",
          "(81, 61, 86, 'B+')",
          "(82, 28, 93, 'A+')",
          "(83, 66, 94, 'E')",
          "(84, 60, 3, 'D+')",
          "(85, 16, 56, 'E')",
          "(86, 58, 49, 'D+')",
          "(87, 73, 71, 'A+')",
          "(88, 32, 71, 'D')",
          "(89, 62, 4, 'A+')",
          "(90, 29, 68, 'C')",
          "(91, 71, 63, 'D')",
          "(92, 6, 24, 'B+')",
          "(93, 44, 90, 'B+')",
          "(94, 30, 3, 'A')",
          "(95, 7, 21, 'E')",
          "(96, 26, 92, 'A-')",
          "(97, 8, 54, 'C+')",
          "(98, 28, 19, 'A')",
          "(99, 73, 97, 'A-')",
          "(100, 30, 12, 'E')"
      };
      for (int i = 0; i < enrollvals.length; i++) {
        connStmt.executeUpdate(stmt + enrollvals[i]);
      }
      System.out.println("ENROLL records inserted.");

    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println("Setup complete.");
  }
}
