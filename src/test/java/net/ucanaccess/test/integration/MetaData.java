package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionDefaultTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;

@RunWith(Parameterized.class)
public class MetaData extends AccessVersionDefaultTest {

    public MetaData(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Override
    public String getAccessPath() {
        return "testdbs/noroman.mdb";
    }

    @Before
    public void beforeTestCase() throws Exception {
        executeStatements("CREATE TABLE AAAn ( baaaa TEXT(3) PRIMARY KEY,A INTEGER , C TEXT(4)) ");
    }

    @After
    public void afterTestCase() throws Exception {
        dropTable("AAAn");
    }

    public void createSimple(String a, Object[][] ver) throws SQLException, IOException {
        try (Statement st = ucanaccess.createStatement()) {
            st.execute("INSERT INTO AAAn VALUES ('33A',11,'" + a + "'   )");
            st.execute("INSERT INTO AAAn VALUES ('33B',111,'" + a + "'    )");
            checkQuery("SELECT * FROM AAAn", ver);
        }
    }

    @Test
    public void testDrop() throws SQLException, IOException {
        Statement st = null;
        ucanaccess.setAutoCommit(false);
        createSimple("a", new Object[][] {{"33A", 11, "a"}, {"33B", 111, "a"}});
        st = ucanaccess.createStatement();
        st.executeUpdate("DROP TABLE AAAn");

        st.execute("CREATE TABLE AAAn ( baaaa TEXT(3) PRIMARY KEY,A INTEGER , C TEXT(4)) ");
        createSimple("b", new Object[][] {{"33A", 11, "b"}, {"33B", 111, "b"}});

        ucanaccess.commit();
        st.close();
    }
}
