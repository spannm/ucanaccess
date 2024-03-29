package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

import java.lang.System.Logger.Level;
import java.sql.ResultSet;
import java.sql.SQLException;

class AliasTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE t_alias (id LONG, descr MEMO, Actuación TEXT)");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testBig(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);
        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            int id = 6666554;
            st.execute("INSERT INTO t_alias (id, descr) VALUES( " + id + ",'t')");
            ResultSet rs = st.executeQuery("SELECT descr AS [cipol%'&la] FROM t_alias WHERE descr<>'ciao'&'bye'&'pippo'");
            rs.next();
            getLogger().log(Level.DEBUG, "metaData columnLabel(1): {0}", rs.getMetaData().getColumnLabel(1));
            getLogger().log(Level.DEBUG, "getObject: {0}", rs.getObject("cipol%'&la"));
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testAccent(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);
        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.execute("INSERT INTO t_alias (id, Actuación) VALUES(1, 'X')");
            ResultSet rs = st.executeQuery("SELECT [Actuación] AS Actuació8_0_0_ FROM t_alias ");
            rs.next();
            getLogger().log(Level.DEBUG, "metaData columnLabel(1): {0}", rs.getMetaData().getColumnLabel(1));
            getLogger().log(Level.DEBUG, "getObject: {0}", rs.getObject("Actuació8_0_0_"));
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testAsin(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);
        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.execute("CREATE TABLE t_asin (asin TEXT, ff TEXT)");
            dumpQueryResult("SELECT asin, ff FROM t_asin");
        }
    }

}
