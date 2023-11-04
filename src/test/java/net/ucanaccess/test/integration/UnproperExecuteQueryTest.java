package net.ucanaccess.test.integration;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import net.ucanaccess.jdbc.UcanaccessSQLException;
import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessBaseTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.SQLException;
import java.sql.Statement;

class UnproperExecuteQueryTest extends UcanaccessBaseTest {

    @Override
    protected String getAccessPath() {
        return TEST_DB_DIR + "noroman.mdb";
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testExecute(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        execute("INSERT INTO t_noroman ([end],[q3¹²³¼½¾ß€Ð×ÝÞðýþäüöß]) VALUES('the end', 'yeeep')");
        execute("UPDATE t_noroman SET [ENd]='BLeah'");
        execute("DELETE FROM t_noroman");
    }

    private void execute(String _sql) throws SQLException {
        try (Statement st = ucanaccess.createStatement()) {
            assertThatThrownBy(() -> st.executeQuery(_sql))
                .isInstanceOf(UcanaccessSQLException.class)
                .hasMessageMatching("UCAExc:::[0-9\\.]+ General error");
            st.execute(_sql);
        }
    }
}
