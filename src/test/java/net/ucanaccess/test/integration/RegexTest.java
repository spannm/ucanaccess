package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionDefaultTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;

@RunWith(Parameterized.class)
public class RegexTest extends AccessVersionDefaultTest {

    public RegexTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Before
    public void beforeTestCase() throws Exception {
        executeStatements("CREATE TABLE reg (id COUNTER,descr memo) ");
    }

    @After
    public void afterTestCase() throws Exception {
        dropTable("reg");
    }

    @Test
    public void testRegex() throws SQLException, IOException {
        Statement st = null;
        st = ucanaccess.createStatement();
        String s = "";
        for (int i = 0; i < 5000; i++) {
            s += "C";
        }

        String[] in = new String[] {"", "\"\"'tCC", s, s + "'", s + "\"", s + "\"''t",
            "\"'\"t" + s, "ss\"1234567890wwwwwwwwww1", "ssss'DDDD", s + "\"\"\"" + s};
        for (String c : in) {
            executeStatement(c);
        }
        String[][] out = new String[in.length * 2][1];
        int k = 0;
        for (int j = 0; j < out.length; j++) {
            out[j][0] = in[k];
            if (j % 2 == 1) {
                k++;
            }
        }
        checkQuery("SELECT descr FROM reg ORDER BY id ASC", out);

        st.close();
    }

    private void executeStatement(String s) throws SQLException {
        try (Statement st = ucanaccess.createStatement()) {
            st.execute(getStatement(s.replaceAll("'", "''"), "'"));

            st.execute(getStatement(s.replaceAll("\"", "\"\""), "\""));

        } catch (SQLException sqle) {
            System.err.println(getStatement(s, "\""));
            System.err
                    .println("converted sql: " + ucanaccess.nativeSQL(getStatement(s.replaceAll("\"", "\"\""), "\"")));
            throw sqle;
        }
    }

    private String getStatement(String _s, String _dlm) {
        return "INSERT INTO reg (descr)  VALUES(  " + _dlm + _s + _dlm + ")";
    }

}
