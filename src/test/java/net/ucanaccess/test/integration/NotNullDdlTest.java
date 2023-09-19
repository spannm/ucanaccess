package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessBaseTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Statement;

class NotNullDdlTest extends UcanaccessBaseTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.test.util.AccessVersion#getDefaultAccessVersion()")
    void confirmNotNullColumnUsingJet(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        // future-proofing in case default file version changes
        assertEquals(getFileFormat().name(), "V2003");

        if (System.getProperty("os.name").startsWith("Windows")) {
            String mdbPath = ucanaccess.getDbIO().getFile().getAbsolutePath();

            Statement st = ucanaccess.createStatement();
            st.execute("CREATE TABLE table1 (id LONG PRIMARY KEY, txt_required TEXT(50) NOT NULL)");
            ucanaccess.close();

            File vbsFile = createTempFileName("NotNullDdlTest", ".vbs");
            vbsFile.deleteOnExit();
            PrintWriter pw = new PrintWriter(vbsFile);
            pw.println("Set conn = CreateObject(\"ADODB.Connection\")");
            pw.println("conn.Open \"DRIVER={Microsoft Access Driver (*.mdb)};DBQ=" + mdbPath + "\"");
            pw.println("conn.Execute \"INSERT INTO table1 (id) VALUES (1)\"");
            pw.println("conn.Close");
            pw.close();

            String cscriptPath = System.getenv("SystemRoot")
                + (System.getProperty("sun.arch.data.model").equals("64") ? "\\SYSWOW64" : "\\SYSTEM32")
                + "\\CSCRIPT.EXE";

            String command = "\"" + cscriptPath + "\" \"" + vbsFile.getAbsolutePath() + "\"";
            Process p = Runtime.getRuntime().exec(command);
            p.waitFor();
            BufferedReader rdr = new BufferedReader(new InputStreamReader(p.getErrorStream()));
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
