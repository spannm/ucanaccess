package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.SQLException;
import java.sql.Statement;

class RegexTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE reg (id COUNTER, descr MEMO) ");
    }

    @AfterEach
    void afterEachTest() throws SQLException {
        executeStatements("DROP TABLE reg");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.type.AccessVersion#getDefaultAccessVersion()")
    void testRegex(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        String s = "C".repeat(5000);
        String[] in = new String[] {"", "\"\"'tCC", s, s + "'", s + "\"", s + "\"''t",
            "\"'\"t" + s, "ss\"1234567890wwwwwwwwww1", "ssss'DDDD", s + "\"\"\"" + s};

        try (Statement st = ucanaccess.createStatement()) {
            for (String c : in) {
                st.execute(getStatement(c.replaceAll("'", "''"), "'"));
                st.execute(getStatement(c.replaceAll("\"", "\"\""), "\""));
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

    private String getStatement(String _s, String _dlm) {
        return "INSERT INTO reg (descr) VALUES( " + _dlm + _s + _dlm + ")";
    }

}
