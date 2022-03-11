package embedded;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import simpledb.jdbc.embedded.EmbeddedDriver;

public class StudentMajor {
   public static void main(String[] args) {
      testStudentDepartmentJoin();
      test4wayJoin();
      testStudentDepartmentCourseJoin();
      testDistinctDepartments();
      testDistinctStudentGradYear();
   }

   public static void testStudentDepartmentJoin() {
      String url = "jdbc:simpledb:studentdb";
      String qry = "select SName, DName "
            + "from DEPT, STUDENT "
            + "where MajorId = DId and SName='amy'";

      Driver d = new EmbeddedDriver();
      try (Connection conn = d.connect(url, null);
            Statement stmt = conn.createStatement()) {

         System.out.println("Name\tMajor");
         ResultSet rs = stmt.executeQuery(qry);
         while (rs.next()) {
            String sname = rs.getString("SName");
            String dname = rs.getString("DName");
            System.out.println(sname + "\t" + dname);
         }
         rs.close();
      } catch (SQLException e) {
         e.printStackTrace();
      }
   }

   public static void testStudentDepartmentCourseJoin() {
      String url = "jdbc:simpledb:studentdb";
      String qry = "select SName, DName, Title "
            + "from DEPT, STUDENT, COURSE "
            + "where MajorId = DId and DId = DeptId";

      Driver d = new EmbeddedDriver();
      try (Connection conn = d.connect(url, null);
            Statement stmt = conn.createStatement()) {

         System.out.println("Name\tMajor\tCourse title");
         ResultSet rs = stmt.executeQuery(qry);
         while (rs.next()) {
            String sname = rs.getString("SName");
            String dname = rs.getString("DName");
            String courseTitle = rs.getString("Title");
            System.out.println(sname + "\t" + dname + "\t" + courseTitle);
         }
         rs.close();
      } catch (SQLException e) {
         e.printStackTrace();
      }
   }

   public static void test4wayJoin() {
      String url = "jdbc:simpledb:studentdb";
      String qry = "select SName, DName, Title, Prof "
            + "from DEPT, STUDENT, COURSE, SECTION "
            + "where MajorId = DId and DId = DeptId and SName = 'amy'";

      Driver d = new EmbeddedDriver();
      try (Connection conn = d.connect(url, null);
            Statement stmt = conn.createStatement()) {

         System.out.println("Name\tMajor\tCourse\tProf");
         ResultSet rs = stmt.executeQuery(qry);
         while (rs.next()) {
            String sname = rs.getString("SName");
            String dname = rs.getString("DName");
            String courseTitle = rs.getString("Title");
            String prof = rs.getString("Prof");
            System.out.println(sname + "\t" + dname + "\t" + courseTitle + "\t" + prof);
         }
         rs.close();
      } catch (SQLException e) {
         e.printStackTrace();
      }
   }

   public static void testDistinctDepartments() {
      String url = "jdbc:simpledb:studentdb";
      String qry = "select distinct DName "
            + "from DEPT";

      Driver d = new EmbeddedDriver();
      try (Connection conn = d.connect(url, null);
            Statement stmt = conn.createStatement()) {

         System.out.println("department");
         ResultSet rs = stmt.executeQuery(qry);
         while (rs.next()) {
            String dname = rs.getString("DName");
            System.out.println(dname);
         }
         rs.close();
      } catch (SQLException e) {
         e.printStackTrace();
      }
   }

   public static void testDistinctStudentGradYear() {
      String url = "jdbc:simpledb:studentdb";
      String qry = "select distinct GradYear "
            + "from STUDENT";

      Driver d = new EmbeddedDriver();
      try (Connection conn = d.connect(url, null);
            Statement stmt = conn.createStatement()) {

         System.out.println("grad year");
         ResultSet rs = stmt.executeQuery(qry);
         while (rs.next()) {
            int gradYear = rs.getInt("GradYear");
            System.out.println(gradYear);
         }
         rs.close();
      } catch (SQLException e) {
         e.printStackTrace();
      }
   }
}
