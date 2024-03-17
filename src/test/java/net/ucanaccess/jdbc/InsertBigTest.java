package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import net.ucanaccess.test.AccessVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class InsertBigTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE t_big (id LONG, descr MEMO)");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testBig(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            String s = IntStream.range(0, 100000).mapToObj(i -> String.format("%05d", i)).collect(Collectors.joining("\r\n"));
            int id = 6666554;
            assertThat(s).hasSizeGreaterThanOrEqualTo(65536);
            st.execute("INSERT INTO t_big (id, descr) VALUES(" + id + ", '" + s + "')");
            ResultSet rs = st.executeQuery("SELECT descr FROM t_big WHERE id=" + id);
            rs.next();
            String retrieved = rs.getString(1);
            assertEquals(s, retrieved);
        }
    }

}
