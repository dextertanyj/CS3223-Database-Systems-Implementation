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
      testDistinctDepartments();
      testDistinctStudentGradYear();
   }

   public static void testStudentDepartmentJoin() {
      String url = "jdbc:simpledb:studentdb";
      String qry = "select SName, DName "
            + "from DEPT, STUDENT "
            + "where MajorId = DId";

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
