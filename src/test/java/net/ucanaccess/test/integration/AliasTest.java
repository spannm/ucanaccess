package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@RunWith(Parameterized.class)
public class AliasTest extends AccessVersionAllTest {

    public AliasTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Before
    public void beforeTestCase() throws Exception {
        executeStatements("CREATE TABLE Talias (id LONG, descr memo, Actuación  text) ");
    }

    @After
    public void dropTable() throws Exception {
        dropTable("Talias");
    }

    @Test
    public void testBig() throws SQLException {
        Statement st = ucanaccess.createStatement();
        int id = 6666554;
        st.execute("INSERT INTO Talias (id,descr) VALUES( " + id + ",'t')");
        ResultSet rs = st.executeQuery("SELECT descr AS [cipol%'&la] FROM Talias where descr<>'ciao'&'bye'&'pippo'");
        rs.next();
        getLogger().debug("metaData columnLabel(1): {}", rs.getMetaData().getColumnLabel(1));
        getLogger().debug("getObject: {}", rs.getObject("cipol%'&la"));

        st.close();
    }

    @Test
    public void testAccent() throws SQLException {
        Statement st = ucanaccess.createStatement();
        st.execute("INSERT INTO Talias (id,Actuación) VALUES(1,'X')");
        ResultSet rs = st.executeQuery("SELECT [Actuación] AS Actuació8_0_0_ FROM Talias ");
        rs.next();
        getLogger().debug("metaData columnLabel(1): {}", rs.getMetaData().getColumnLabel(1));
        getLogger().debug("getObject: {}", rs.getObject("Actuació8_0_0_"));
        st.close();
    }

    @Test
    public void testAsin() throws SQLException, IOException {
        Statement st = ucanaccess.createStatement();
        st.execute("CREATE TABLE xxxx (asin text, ff text)");
        dumpQueryResult("SELECT asin, ff FROM xxxx");
        st.close();
    }

}
