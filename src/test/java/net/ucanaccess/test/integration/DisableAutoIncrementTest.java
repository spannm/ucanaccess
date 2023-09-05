package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionDefaultTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;

@RunWith(Parameterized.class)
public class DisableAutoIncrementTest extends AccessVersionDefaultTest {

    public DisableAutoIncrementTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Before
    public void beforeTestCase() throws Exception {
        executeStatements("CREATE TABLE CT (id COUNTER PRIMARY KEY ,descr TEXT) ",
                "CREATE TABLE [C T] (id COUNTER PRIMARY KEY ,descr TEXT) ");
    }

    @Test
    public void testGuid() throws SQLException, IOException {
        Statement st = ucanaccess.createStatement();
        st.execute("CREATE TABLE CT1 (id guid PRIMARY KEY ,descr TEXT) ");
        st.execute("INSERT INTO CT1 (descr) VALUES ('CIAO')");

        checkQuery("SELECT * FROM CT1");
        st.close();
    }

    @Test
    public void testDisable() throws SQLException, IOException {
        Statement st = null;
        boolean exc = false;
        st = ucanaccess.createStatement();
        st.execute("INSERT INTO CT (descr) VALUES ('CIAO')");
        st.execute("DISABLE AUTOINCREMENT ON CT ");
        try {
            st.execute("INSERT INTO CT (descr) VALUES ('CIAO')");
        } catch (Exception e) {
            exc = true;
        }
        assertTrue(exc);
        st.execute("enable AUTOINCREMENT ON CT ");
        st.execute("INSERT INTO CT (descr) VALUES ('CIAO')");
        st.execute("DISABLE AUTOINCREMENT ON[C T]");
        st.close();
    }

}
