package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

class RegexTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE reg (id COUNTER, descr MEMO) ");
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
                executeStatements(st,
                    getStatement(c.replaceAll("'", "''"), "'"),
                    getStatement(c.replaceAll("\"", "\"\""), "\""));
            }

            int len = in.length * 2;
            List<List<Object>> out = new ArrayList<>(len);
            for (int i = 0, k = 0; i < len; i++) {
                out.add(List.of(in[k]));
                if (i % 2 == 1) {
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
