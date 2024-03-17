package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessDefaultVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

class RegexTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE t_regex (id COUNTER, descr MEMO) ");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessDefaultVersionSource
    void testRegex(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        String s = "C".repeat(5000);
        String[] in = new String[] {"", "\"\"'tCC", s, s + "'", s + "\"", s + "\"''t",
            "\"'\"t" + s, "ss\"1234567890wwwwwwwwww1", "ssss'DDDD", s + "\"\"\"" + s};

        try (UcanaccessStatement st = ucanaccess.createStatement()) {
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
            checkQuery("SELECT descr FROM t_regex ORDER BY id ASC", out);
        }
    }

    private String getStatement(CharSequence _s, CharSequence _dlm) {
        return "INSERT INTO t_regex (descr) VALUES( " + _dlm + _s + _dlm + ")";
    }

}
