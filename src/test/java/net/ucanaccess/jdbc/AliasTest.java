package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import net.ucanaccess.test.AccessVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.ResultSet;
import java.sql.SQLException;

class AliasTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion accessVersion) throws SQLException {
        super.init(accessVersion);
        executeStatements("CREATE TABLE t_alias (id LONG, descr MEMO, Actuación TEXT)");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testBig(AccessVersion accessVersion) throws SQLException {
        init(accessVersion);
        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            int id = 6666554;
            st.execute("INSERT INTO t_alias (id, descr) VALUES( " + id + ",'t')");
            ResultSet rs = st.executeQuery("SELECT descr AS [cipol%'&la] FROM t_alias WHERE descr<>'ciao'&'bye'&'pippo'");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getMetaData().getColumnLabel(1)).isEqualTo("cipol%'&la");
            assertThat(rs.getObject("cipol%'&la")).isEqualTo("t");
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testAccent(AccessVersion accessVersion) throws SQLException {
        init(accessVersion);
        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.execute("INSERT INTO t_alias (id, Actuación) VALUES(1, 'X')");
            ResultSet rs = st.executeQuery("SELECT [Actuación] AS Actuació8_0_0_ FROM t_alias ");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getMetaData().getColumnLabel(1)).isEqualTo("Actuació8_0_0_");
            assertThat(rs.getObject("Actuació8_0_0_")).isEqualTo("X");
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testAsin(AccessVersion accessVersion) throws SQLException {
        init(accessVersion);
        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.execute("CREATE TABLE t_asin (asin TEXT, ff TEXT)");
            st.execute("INSERT INTO t_asin (asin, ff) VALUES ('a', 'f')");
            checkQuery("SELECT asin, ff FROM t_asin", singleRec("a", "f"));
        }
    }

}
