package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionDefaultTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Statement;

@RunWith(Parameterized.class)
public class NotNullDdlTest extends AccessVersionDefaultTest {

    public NotNullDdlTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Test
    public void confirmNotNullColumnUsingJet() throws Exception {
        // future-proofing in case default file version changes
        assertEquals(fileFormat.name(), "V2003");

        if (System.getProperty("os.name").startsWith("Windows")) {
            String mdbPath = ucanaccess.getDbIO().getFile().getAbsolutePath();

            Statement st = ucanaccess.createStatement();
            st.execute("CREATE TABLE table1 (id LONG PRIMARY KEY, txt_required TEXT(50) NOT NULL)");
            ucanaccess.close();

            File vbsFile = File.createTempFile("NotNullDdlTest", ".vbs", TEST_DB_TEMP_DIR);
            vbsFile.deleteOnExit();
            PrintWriter pw = new PrintWriter(vbsFile);
            pw.println("Set conn = CreateObject(\"ADODB.Connection\")");
            pw.println("conn.Open \"DRIVER={Microsoft Access Driver (*.mdb)};DBQ=" + mdbPath + "\"");
            pw.println("conn.Execute \"INSERT INTO table1 (id) VALUES (1)\"");
            pw.println("conn.Close");
            pw.close();

            String cscriptPath = new StringBuffer(System.getenv("SystemRoot"))
                .append(System.getProperty("sun.arch.data.model").equals("64") ? "\\SYSWOW64" : "\\SYSTEM32")
                .append("\\CSCRIPT.EXE").toString();

            String command = "\"" + cscriptPath + "\" \"" + vbsFile.getAbsolutePath() + "\"";
            Process p = Runtime.getRuntime().exec(command);
            p.waitFor();
            BufferedReader rdr =
                new BufferedReader(new InputStreamReader(p.getErrorStream()));
            StringBuilder sb = new StringBuilder(); // to capture error line(s)
            int numErrorLines = 0;
            String line = rdr.readLine();
            while (line != null) {
                numErrorLines++;
                getLogger().info(line);
                sb.append(String.format("%s%n", line));
                line = rdr.readLine();
            }
            if (numErrorLines == 0) {
                fail("The VBScript should have thrown an error, but it did not.");
            } else {
                if (!sb.toString().contains("table1.txt_required")) {
                    fail("The VBScript threw an unexpected error.");
                }
            }
        }
    }
}
