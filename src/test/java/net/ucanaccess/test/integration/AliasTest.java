package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessBaseTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

class AliasTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE Talias (id LONG, descr MEMO, Actuación TEXT)");
    }

    @AfterEach
    void afterEachTest() throws Exception {
        dropTable("Talias");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testBig(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);
        try (Statement st = ucanaccess.createStatement()) {
            int id = 6666554;
            st.execute("INSERT INTO Talias (id, descr) VALUES( " + id + ",'t')");
            ResultSet rs = st.executeQuery("SELECT descr AS [cipol%'&la] FROM Talias WHERE descr<>'ciao'&'bye'&'pippo'");
            rs.next();
            getLogger().debug("metaData columnLabel(1): {}", rs.getMetaData().getColumnLabel(1));
            getLogger().debug("getObject: {}", rs.getObject("cipol%'&la"));
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testAccent(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);
        try (Statement st = ucanaccess.createStatement()) {
            st.execute("INSERT INTO Talias (id, Actuación) VALUES(1, 'X')");
            ResultSet rs = st.executeQuery("SELECT [Actuación] AS Actuació8_0_0_ FROM Talias ");
            rs.next();
            getLogger().debug("metaData columnLabel(1): {}", rs.getMetaData().getColumnLabel(1));
            getLogger().debug("getObject: {}", rs.getObject("Actuació8_0_0_"));
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testAsin(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);
        try (Statement st = ucanaccess.createStatement()) {
            st.execute("CREATE TABLE xxxx (asin TEXT, ff TEXT)");
            dumpQueryResult("SELECT asin, ff FROM xxxx");
        }
    }

}
