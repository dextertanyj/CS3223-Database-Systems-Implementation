package test.system;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import static java.sql.Types.INTEGER;

import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import test.integration.TestUtils;

public class SimpleDBTest {
    private static String fileDirectory = System.getProperty("user.dir");
    public static void main(String args[]) {
        if (!(new File(fileDirectory + "/studenttestdb").exists())) {
            TestUtils.systemSetup("studenttestdb");
        }
        SimpleDB db = new SimpleDB("studenttestdb");
        
        
        // Parse query file
        
        try {
            
            Files.delete(Paths.get(fileDirectory + "/result.txt"));
            File file = new File(fileDirectory + "/query.txt");
            BufferedReader br = new BufferedReader(new FileReader(file));
            String query;
            while ((query = br.readLine()) != null) {
                doQuery(db.planner(), db.newTx(), query);
            }
            
        } catch(Exception e) {
            System.out.println(e);
        }
    }
    
    public static void doQuery(Planner planner, Transaction tx, String cmd) {
        Plan plan = planner.createQueryPlan(cmd, tx);
        try {
            Schema schema = plan.schema();
            List<String> columns = schema.fields();
            int columncount = columns.size();
            
            BufferedWriter buffWriter = new BufferedWriter(new FileWriter(fileDirectory + "/result.txt", true));
            
            for (int i = 0; i < columncount; i++) {
                String fldname = columns.get(i);
                buffWriter.append(fldname);
                if (i != columncount - 1) {
                    buffWriter.append(' ');
                }
            }
            buffWriter.newLine();
            
            Scan s = plan.open();
            // print records
            while (s.next()) {
                for (int i = 0; i < columncount; i++) {
                    String fldname = columns.get(i);
                    int fldtype = schema.type(fldname);
                    String val = "";
                    if (fldtype == INTEGER) {
                        int ival = s.getInt(fldname);
                        val = String.valueOf(ival) + " ";
                    } else {
                        val = s.getString(fldname + " ");
                    }
                    if (i == columncount - 1) {
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
            s.close();
            tx.commit();
        } catch (Exception e) {
            System.out.println("Exception: " + e.getMessage());
            tx.rollback();
        }
    }
}
