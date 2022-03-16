package test.system;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import static java.sql.Types.INTEGER;

public class JDBCResults {
    private static String fileDirectory = System.getProperty("user.dir");
    static final String DB_URL = "jdbc:mysql://localhost/testing";
    static final String USER = "guest";
    static final String PASS = "guest";
    
    public static void main(String[] args) {
        System.out.println("Connecting to a selected database...");
        // Open a connection
        int queryNum = 1;
        try(Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);) {		      
            System.out.println("Connected database successfully...");  
            ResultSet resultSet = conn.getMetaData().getCatalogs();
            resultSet.close();
            // drop tables
            Statement stmt = conn.createStatement();
            String sqlString = "drop table student";
            stmt.executeUpdate(sqlString);
            sqlString = "drop table dept";
            stmt.executeUpdate(sqlString);
            sqlString = "drop table course";
            stmt.executeUpdate(sqlString);
            sqlString = "drop table section";
            stmt.executeUpdate(sqlString);
            sqlString = "drop table enroll";
            stmt.executeUpdate(sqlString);
            
            // create table
            sqlString = "create table student(SId int, SName varchar(10), MajorId int, GradYear int)";
            stmt.executeUpdate(sqlString);
            sqlString = "create table dept(DId int, DName varchar(8))";
            stmt.executeUpdate(sqlString);
            sqlString = "create table course(CId int, Title varchar(20), DeptId int)";
            stmt.executeUpdate(sqlString);
            sqlString = "create table section(SectId int, CourseId int, Prof varchar(8), YearOffered int)";
            stmt.executeUpdate(sqlString);
            sqlString = "create table enroll(EId int, StudentId int, SectionId int, Grade varchar(2))";
            stmt.executeUpdate(sqlString);
            
            // insert data
            sqlString = "insert into student(SId, SName, MajorId, GradYear) values ";
            String[] studVals = {"(1, 'aaa', 6, 2020)",
            "(2, 'aab', 2, 2020)",
            "(3, 'aac', 4, 2021)",
            "(4, 'aad', 9, 2019)",
            "(5, 'aae', 10, 2021)",
            "(6, 'aaf', 8, 2020)",
            "(7, 'aag', 5, 2020)",
            "(8, 'aah', 10, 2020)",
            "(9, 'aai', 9, 2020)",
            "(10, 'aaj', 1, 2020)",
            "(11, 'aak', 9, 2019)",
            "(12, 'aal', 10, 2021)",
            "(13, 'aam', 3, 2021)",
            "(14, 'aan', 4, 2020)",
            "(15, 'aao', 3, 2021)",
            "(16, 'aap', 7, 2021)",
            "(17, 'aaq', 7, 2020)",
            "(18, 'aar', 7, 2021)",
            "(19, 'aas', 9, 2020)",
            "(20, 'aat', 10, 2019)",
            "(21, 'aau', 6, 2019)",
            "(22, 'aav', 4, 2020)",
            "(23, 'aaw', 2, 2020)",
            "(24, 'aax', 9, 2020)",
            "(25, 'aay', 2, 2021)",
            "(26, 'aaz', 9, 2021)",
            "(27, 'aba', 2, 2019)",
            "(28, 'abb', 6, 2019)",
            "(29, 'abc', 10, 2021)",
            "(30, 'abd', 4, 2019)",
            "(31, 'abe', 9, 2020)",
            "(32, 'abf', 10, 2019)",
            "(33, 'abg', 5, 2021)",
            "(34, 'abh', 5, 2021)",
            "(35, 'abi', 10, 2020)",
            "(36, 'abj', 1, 2019)",
            "(37, 'abk', 4, 2021)",
            "(38, 'abl', 10, 2020)",
            "(39, 'abm', 1, 2021)",
            "(40, 'abn', 4, 2019)",
            "(41, 'abo', 2, 2019)",
            "(42, 'abp', 7, 2020)",
            "(43, 'abq', 8, 2021)",
            "(44, 'abr', 8, 2019)",
            "(45, 'abs', 10, 2021)",
            "(46, 'abt', 8, 2020)",
            "(47, 'abu', 5, 2020)",
            "(48, 'abv', 1, 2021)",
            "(49, 'abw', 9, 2019)",
            "(50, 'abx', 2, 2021)",
            "(51, 'aby', 8, 2019)",
            "(52, 'abz', 1, 2019)",
            "(53, 'aca', 4, 2020)",
            "(54, 'acb', 2, 2020)",
            "(55, 'acc', 8, 2021)",
            "(56, 'acd', 6, 2019)",
            "(57, 'ace', 10, 2019)",
            "(58, 'acf', 5, 2020)",
            "(59, 'acg', 9, 2020)",
            "(60, 'ach', 4, 2021)",
            "(61, 'aci', 5, 2021)",
            "(62, 'acj', 9, 2021)",
            "(63, 'ack', 2, 2019)",
            "(64, 'acl', 8, 2019)",
            "(65, 'acm', 4, 2021)",
            "(66, 'acn', 7, 2021)",
            "(67, 'aco', 3, 2021)",
            "(68, 'acp', 5, 2020)",
            "(69, 'acq', 8, 2021)",
            "(70, 'acr', 9, 2021)",
            "(71, 'acs', 10, 2020)",
            "(72, 'act', 5, 2020)",
            "(73, 'acu', 9, 2020)",
            "(74, 'acv', 6, 2020)",
            "(75, 'acw', 5, 2019)",
            "(76, 'acx', 2, 2019)",
            "(77, 'acy', 7, 2020)",
            "(78, 'acz', 7, 2019)",
            "(79, 'ada', 3, 2019)",
            "(80, 'adb', 9, 2020)",
            "(81, 'adc', 10, 2020)",
            "(82, 'add', 3, 2021)",
            "(83, 'ade', 7, 2019)",
            "(84, 'adf', 2, 2019)",
            "(85, 'adg', 6, 2021)",
            "(86, 'adh', 1, 2020)",
            "(87, 'adi', 4, 2021)",
            "(88, 'adj', 1, 2019)",
            "(89, 'adk', 8, 2020)",
            "(90, 'adl', 2, 2020)",
            "(91, 'adm', 4, 2021)",
            "(92, 'adn', 10, 2019)",
            "(93, 'ado', 1, 2020)",
            "(94, 'adp', 8, 2021)",
            "(95, 'adq', 5, 2019)",
            "(96, 'adr', 5, 2019)",
            "(97, 'ads', 2, 2020)",
            "(98, 'adt', 3, 2020)",
            "(99, 'adu', 7, 2020)",
            "(100, 'adv', 4, 2020)",
            "(101, 'adw', 3, 2019)",
            "(102, 'adx', 8, 2020)",
            "(103, 'ady', 1, 2020)",
            "(104, 'adz', 3, 2021)",
            "(105, 'aea', 2, 2021)",
            "(106, 'aeb', 1, 2019)",
            "(107, 'aec', 2, 2020)",
            "(108, 'aed', 1, 2020)",
            "(109, 'aee', 10, 2020)",
            "(110, 'aef', 4, 2020)",
            "(111, 'aeg', 1, 2021)",
            "(112, 'aeh', 2, 2020)",
            "(113, 'aei', 6, 2019)",
            "(114, 'aej', 3, 2020)",
            "(115, 'aek', 5, 2019)",
            "(116, 'ael', 9, 2019)",
            "(117, 'aem', 3, 2021)",
            "(118, 'aen', 10, 2020)",
            "(119, 'aeo', 4, 2019)",
            "(120, 'aep', 4, 2020)",
            "(121, 'aeq', 2, 2019)",
            "(122, 'aer', 3, 2021)",
            "(123, 'aes', 9, 2019)",
            "(124, 'aet', 1, 2021)",
            "(125, 'aeu', 1, 2021)",
            "(126, 'aev', 2, 2020)",
            "(127, 'aew', 5, 2021)",
            "(128, 'aex', 4, 2021)",
            "(129, 'aey', 1, 2019)",
            "(130, 'aez', 2, 2019)",
            "(131, 'afa', 2, 2020)",
            "(132, 'afb', 5, 2019)",
            "(133, 'afc', 10, 2019)",
            "(134, 'afd', 1, 2020)",
            "(135, 'afe', 10, 2020)",
            "(136, 'aff', 7, 2020)",
            "(137, 'afg', 6, 2021)",
            "(138, 'afh', 1, 2020)",
            "(139, 'afi', 1, 2021)",
            "(140, 'afj', 7, 2021)",
            "(141, 'afk', 7, 2021)",
            "(142, 'afl', 2, 2019)",
            "(143, 'afm', 8, 2021)",
            "(144, 'afn', 5, 2021)",
            "(145, 'afo', 4, 2020)",
            "(146, 'afp', 8, 2020)",
            "(147, 'afq', 7, 2021)",
            "(148, 'afr', 1, 2019)",
            "(149, 'afs', 5, 2021)",
            "(150, 'aft', 7, 2020)",
            "(151, 'afu', 8, 2019)",
            "(152, 'afv', 9, 2019)",
            "(153, 'afw', 2, 2019)",
            "(154, 'afx', 3, 2021)",
            "(155, 'afy', 6, 2020)",
            "(156, 'afz', 5, 2021)",
            "(157, 'aga', 9, 2020)",
            "(158, 'agb', 4, 2019)",
            "(159, 'agc', 9, 2020)",
            "(160, 'agd', 7, 2020)",
            "(161, 'age', 10, 2021)",
            "(162, 'agf', 4, 2021)",
            "(163, 'agg', 9, 2020)",
            "(164, 'agh', 1, 2021)",
            "(165, 'agi', 1, 2020)",
            "(166, 'agj', 5, 2021)",
            "(167, 'agk', 9, 2021)",
            "(168, 'agl', 3, 2020)",
            "(169, 'agm', 6, 2019)",
            "(170, 'agn', 5, 2019)",
            "(171, 'ago', 9, 2020)",
            "(172, 'agp', 4, 2021)",
            "(173, 'agq', 10, 2019)",
            "(174, 'agr', 1, 2019)",
            "(175, 'ags', 6, 2021)",
            "(176, 'agt', 4, 2020)",
            "(177, 'agu', 10, 2021)",
            "(178, 'agv', 4, 2021)",
            "(179, 'agw', 8, 2021)",
            "(180, 'agx', 4, 2020)",
            "(181, 'agy', 6, 2021)",
            "(182, 'agz', 2, 2019)",
            "(183, 'aha', 8, 2021)",
            "(184, 'ahb', 8, 2020)",
            "(185, 'ahc', 9, 2019)",
            "(186, 'ahd', 6, 2019)",
            "(187, 'ahe', 5, 2020)",
            "(188, 'ahf', 8, 2020)",
            "(189, 'ahg', 9, 2019)",
            "(190, 'ahh', 4, 2021)",
            "(191, 'ahi', 1, 2019)",
            "(192, 'ahj', 8, 2019)",
            "(193, 'ahk', 5, 2021)",
            "(194, 'ahl', 4, 2019)",
            "(195, 'ahm', 7, 2020)",
            "(196, 'ahn', 6, 2019)",
            "(197, 'aho', 10, 2020)",
            "(198, 'ahp', 7, 2021)",
            "(199, 'ahq', 3, 2020)",
            "(200, 'ahr', 7, 2019)"};
            
            for (String s : studVals) {
                stmt.executeUpdate(sqlString + s);
            }
            
            sqlString = "insert into dept(DId, DName) values ";
            String[] deptVals = {"(1, 'compsci')", 
            "(2, 'math')",
            "(3, 'drama')",
            "(4, 'physics')",
            "(5, 'chem')",
            "(6, 'geog')",
            "(7, 'law')",
            "(8, 'med')",
            "(9, 'biz')",
            "(10, 'psych')",
            "(11, 'lang')"};
            for (String s : deptVals) {
                stmt.executeUpdate(sqlString + s);
            }
            
            sqlString = "insert into course(CId, Title, DeptId) values ";
            String[] courseVals = {
                "(1, 'db systems', 1)",
                "(2, 'compilers', 1)",
                "(3, 'calculus systems', 2)",
                "(4, 'algebra', 2)",
                "(5, 'acting', 3)",
                "(6, 'elocution', 3)",
                "(7, 'thermodynamics', 4)",
                "(8, 'forces', 4)",
                "(9, 'organic', 5)",
                "(10, 'bonding', 5)",
                "(11, 'human', 6)",
                "(12, 'sea', 6)",
                "(13, 'bankruptcy', 7)",
                "(14, 'criminal', 7)",
                "(15, 'cardiovascular', 8)",
                "(16, 'gastroenterology', 8)",
                "(17, 'finance', 9)",
                "(18, 'accounting', 9)",
                "(19, 'communication', 10)",
                "(20, 'perception', 10)",
                "(21, 'japanese', 11)"
            };
            for (String s : courseVals) {
                stmt.executeUpdate(sqlString + s);
            }
            
            sqlString = "insert into section(SectId, CourseId, Prof, YearOffered) values ";
            String[] sectionVals = {
                "(13, 1, 'turing', 2018)",
                "(23, 1, 'turing', 2016)",
                "(33, 2, 'newton', 2017)",
                "(43, 2, 'eistein', 2018)",
                "(53, 3, 'brando', 2017)",
                "(63, 4, 'lee', 2020)",
            };
            for (String s : sectionVals) {
                stmt.executeUpdate(sqlString + s);
            }
            
            sqlString = "insert into enroll(EId, StudentId, SectionId, Grade) values ";
            String[] enrollVals = {"(1, 125, 23, 'A')",
            "(2, 148, 43, 'A')",
            "(3, 39, 23, 'A')",
            "(4, 68, 13, 'C')",
            "(5, 139, 43, 'B')",
            "(6, 195, 23, 'C')",
            "(7, 15, 13, 'D')",
            "(8, 58, 13, 'A')",
            "(9, 194, 13, 'D')",
            "(10, 124, 23, 'C')",
            "(11, 159, 53, 'D')",
            "(12, 119, 43, 'A')",
            "(13, 120, 53, 'A')",
            "(14, 177, 33, 'C')",
            "(15, 15, 33, 'A')",
            "(16, 198, 33, 'C')",
            "(17, 155, 53, 'A')",
            "(18, 165, 23, 'C')",
            "(19, 44, 43, 'B')",
            "(20, 190, 23, 'C')",
            "(21, 91, 53, 'B')",
            "(22, 130, 53, 'C')",
            "(23, 151, 43, 'C')",
            "(24, 128, 23, 'C')",
            "(25, 116, 13, 'A')",
            "(26, 17, 53, 'B')",
            "(27, 127, 43, 'D')",
            "(28, 102, 33, 'A')",
            "(29, 38, 53, 'A')",
            "(30, 39, 53, 'B')",
            "(31, 111, 53, 'D')",
            "(32, 151, 13, 'A')",
            "(33, 197, 13, 'A')",
            "(34, 55, 33, 'D')",
            "(35, 51, 23, 'A')",
            "(36, 148, 23, 'A')",
            "(37, 19, 53, 'B')",
            "(38, 112, 23, 'A')",
            "(39, 75, 43, 'B')",
            "(40, 9, 13, 'A')",
            "(41, 125, 13, 'D')",
            "(42, 185, 13, 'A')",
            "(43, 71, 23, 'A')",
            "(44, 89, 13, 'C')",
            "(45, 173, 53, 'C')",
            "(46, 65, 43, 'A')",
            "(47, 85, 13, 'C')",
            "(48, 154, 43, 'D')",
            "(49, 127, 33, 'C')",
            "(50, 170, 33, 'D')",
            "(51, 20, 43, 'D')",
            "(52, 191, 23, 'D')",
            "(53, 34, 13, 'D')",
            "(54, 164, 53, 'D')",
            "(55, 143, 13, 'B')",
            "(56, 93, 13, 'C')",
            "(57, 114, 53, 'B')",
            "(58, 66, 13, 'D')",
            "(59, 125, 13, 'C')",
            "(60, 105, 23, 'B')",
            "(61, 67, 53, 'B')",
            "(62, 133, 43, 'B')",
            "(63, 105, 23, 'C')",
            "(64, 74, 13, 'A')",
            "(65, 68, 43, 'D')",
            "(66, 17, 53, 'A')",
            "(67, 154, 33, 'C')",
            "(68, 150, 13, 'C')",
            "(69, 97, 53, 'A')",
            "(70, 161, 43, 'B')",
            "(71, 181, 13, 'B')",
            "(72, 64, 13, 'A')",
            "(73, 152, 33, 'A')",
            "(74, 178, 53, 'C')",
            "(75, 36, 53, 'B')",
            "(76, 122, 53, 'B')",
            "(77, 150, 43, 'D')",
            "(78, 75, 43, 'A')",
            "(79, 120, 23, 'A')",
            "(80, 187, 23, 'A')",
            "(81, 55, 23, 'D')",
            "(82, 87, 23, 'D')",
            "(83, 116, 53, 'C')",
            "(84, 49, 53, 'B')",
            "(85, 147, 43, 'C')",
            "(86, 65, 53, 'A')",
            "(87, 95, 13, 'B')",
            "(88, 168, 33, 'A')",
            "(89, 39, 33, 'A')",
            "(90, 141, 43, 'A')",
            "(91, 128, 23, 'B')",
            "(92, 73, 43, 'B')",
            "(93, 72, 23, 'A')",
            "(94, 49, 13, 'D')",
            "(95, 145, 43, 'C')",
            "(96, 144, 33, 'C')",
            "(97, 153, 43, 'C')",
            "(98, 45, 23, 'B')",
            "(99, 127, 53, 'B')",
            "(100, 23, 53, 'B')",
            "(101, 97, 23, 'B')",
            "(102, 34, 53, 'A')",
            "(103, 111, 33, 'C')",
            "(104, 10, 23, 'D')",
            "(105, 121, 23, 'C')",
            "(106, 58, 43, 'B')",
            "(107, 18, 13, 'D')",
            "(108, 166, 33, 'B')",
            "(109, 134, 23, 'D')",
            "(110, 137, 53, 'D')",
            "(111, 193, 13, 'B')",
            "(112, 164, 53, 'D')",
            "(113, 52, 33, 'C')",
            "(114, 82, 13, 'C')",
            "(115, 105, 13, 'D')",
            "(116, 106, 23, 'B')",
            "(117, 176, 43, 'B')",
            "(118, 12, 23, 'D')",
            "(119, 6, 43, 'A')",
            "(120, 197, 43, 'D')",
            "(121, 17, 43, 'D')",
            "(122, 143, 53, 'B')",
            "(123, 155, 43, 'B')",
            "(124, 56, 13, 'C')",
            "(125, 25, 43, 'A')",
            "(126, 155, 23, 'B')",
            "(127, 34, 43, 'D')",
            "(128, 29, 23, 'C')",
            "(129, 109, 23, 'D')",
            "(130, 78, 13, 'C')",
            "(131, 23, 33, 'C')",
            "(132, 141, 43, 'B')",
            "(133, 184, 13, 'C')",
            "(134, 57, 13, 'D')",
            "(135, 52, 33, 'C')",
            "(136, 117, 23, 'D')",
            "(137, 40, 13, 'D')",
            "(138, 174, 13, 'D')",
            "(139, 185, 33, 'A')",
            "(140, 96, 43, 'C')",
            "(141, 98, 53, 'B')",
            "(142, 16, 43, 'B')",
            "(143, 62, 23, 'D')",
            "(144, 23, 33, 'C')",
            "(145, 172, 33, 'A')",
            "(146, 78, 33, 'B')",
            "(147, 194, 13, 'D')",
            "(148, 156, 13, 'C')",
            "(149, 177, 23, 'B')",
            "(150, 70, 53, 'D')",
            "(151, 21, 43, 'D')",
            "(152, 30, 23, 'C')",
            "(153, 43, 23, 'C')",
            "(154, 56, 13, 'B')",
            "(155, 20, 33, 'D')",
            "(156, 137, 23, 'A')",
            "(157, 108, 33, 'C')",
            "(158, 5, 43, 'A')",
            "(159, 102, 33, 'C')",
            "(160, 49, 33, 'A')",
            "(161, 168, 43, 'C')",
            "(162, 51, 43, 'B')",
            "(163, 34, 43, 'D')",
            "(164, 169, 23, 'A')",
            "(165, 70, 13, 'B')",
            "(166, 112, 53, 'C')",
            "(167, 15, 13, 'A')",
            "(168, 105, 33, 'D')",
            "(169, 116, 43, 'A')",
            "(170, 86, 13, 'B')",
            "(171, 152, 23, 'B')",
            "(172, 155, 43, 'C')",
            "(173, 200, 33, 'B')",
            "(174, 20, 13, 'D')",
            "(175, 149, 43, 'A')",
            "(176, 78, 43, 'A')",
            "(177, 102, 53, 'B')",
            "(178, 74, 33, 'A')",
            "(179, 45, 13, 'B')",
            "(180, 156, 13, 'A')",
            "(181, 99, 33, 'A')",
            "(182, 44, 33, 'A')",
            "(183, 192, 13, 'B')",
            "(184, 191, 13, 'D')",
            "(185, 40, 33, 'B')",
            "(186, 178, 53, 'D')",
            "(187, 137, 13, 'D')",
            "(188, 36, 23, 'B')",
            "(189, 142, 43, 'C')",
            "(190, 104, 33, 'A')",
            "(191, 198, 13, 'A')",
            "(192, 10, 13, 'A')",
            "(193, 17, 23, 'A')",
            "(194, 176, 23, 'D')",
            "(195, 112, 23, 'B')",
            "(196, 110, 43, 'C')",
            "(197, 27, 33, 'C')",
            "(198, 98, 33, 'A')",
            "(199, 93, 13, 'C')",
            "(200, 24, 33, 'C')"};
            for (String s : enrollVals) {
                stmt.executeUpdate(sqlString + s);
            }
            
            // query results
            Files.delete(Paths.get(fileDirectory + "/answer.txt"));
            File file = new File(fileDirectory + "/query.txt");
            BufferedReader br = new BufferedReader(new FileReader(file));
            String query;
            while ((query = br.readLine()) != null) {
                ResultSet rs = stmt.executeQuery(query);
                outputResultSet(rs);
                queryNum++;
            }
            System.out.println("Results generation completed");
        } catch (Exception e) {
            System.out.println("Query " + queryNum + ": ");
            e.printStackTrace();
        } 
    }
    public static void outputResultSet(ResultSet rs) {
        try {
            BufferedWriter buffWriter = new BufferedWriter(new FileWriter(fileDirectory + "/answer.txt", true));
            ResultSetMetaData rsMetaData = rs.getMetaData();
            for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
                String fieldName = rsMetaData.getColumnName(i);
                buffWriter.append(fieldName);
                if (i != rsMetaData.getColumnCount()) {
                    buffWriter.append(' ');
                }
            }
            buffWriter.newLine();
            
            while (rs.next()) {
                for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
                    int fieldType = rsMetaData.getColumnType(i);
                    String val = "";
                    if (fieldType == INTEGER) {
                        int ival = rs.getInt(i);
                        val = String.valueOf(ival)+ " ";
                    } else {
                        String sval = rs.getString(i);
                        val = sval + " ";
                    }
                    if (i == rsMetaData.getColumnCount()) {
                        val = val.substring(0, val.length() - 1);
                    }
                    buffWriter.append(val);
                }
                buffWriter.newLine();
            }
            buffWriter.append('-');
            buffWriter.newLine();
            buffWriter.flush();
            buffWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
