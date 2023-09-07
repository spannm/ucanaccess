package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessTestBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;

class DisableAutoIncrementTest extends UcanaccessTestBase {

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE CT (id COUNTER PRIMARY KEY ,descr TEXT) ",
                "CREATE TABLE [C T] (id COUNTER PRIMARY KEY ,descr TEXT) ");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.test.util.AccessVersion#getDefaultAccessVersion()")
    void testGuid(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);
        Statement st = ucanaccess.createStatement();
        st.execute("CREATE TABLE CT1 (id guid PRIMARY KEY ,descr TEXT) ");
        st.execute("INSERT INTO CT1 (descr) VALUES ('CIAO')");

        checkQuery("SELECT * FROM CT1");
        st.close();
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.test.util.AccessVersion#getDefaultAccessVersion()")
    void testDisable(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);
        boolean exc = false;
        Statement st = ucanaccess.createStatement();
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
