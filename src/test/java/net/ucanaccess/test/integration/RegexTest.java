package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessBaseTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;

class RegexTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE reg (id COUNTER,descr memo) ");
    }

    @AfterEach
    void afterEachTest() throws SQLException {
        dropTable("reg");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.test.util.AccessVersion#getDefaultAccessVersion()")
    void testRegex(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);

        String s = "C".repeat(5000);
        String[] in = new String[] {"", "\"\"'tCC", s, s + "'", s + "\"", s + "\"''t",
            "\"'\"t" + s, "ss\"1234567890wwwwwwwwww1", "ssss'DDDD", s + "\"\"\"" + s};

        try (Statement st = ucanaccess.createStatement()) {
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
        }
    }

    private void executeStatement(String s) throws SQLException {
        try (Statement st = ucanaccess.createStatement()) {
            st.execute(getStatement(s.replaceAll("'", "''"), "'"));

            st.execute(getStatement(s.replaceAll("\"", "\"\""), "\""));

        } catch (SQLException sqle) {
            System.err.println(getStatement(s, "\""));
            System.err.println("Converted sql: " + ucanaccess.nativeSQL(getStatement(s.replaceAll("\"", "\"\""), "\"")));
            throw sqle;
        }
    }

    private String getStatement(String _s, String _dlm) {
        return "INSERT INTO reg (descr)  VALUES(  " + _dlm + _s + _dlm + ")";
    }

}
