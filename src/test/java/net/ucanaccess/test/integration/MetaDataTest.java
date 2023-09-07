package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;

class MetaDataTest extends UcanaccessTestBase {

    @Override
    protected String getAccessPath() {
        return TEST_DB_DIR + "noroman.mdb";
    }

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE AAAn ( baaaa TEXT(3) PRIMARY KEY,A INTEGER , C TEXT(4)) ");
    }

    @AfterEach
    void afterEachTest() throws SQLException {
        dropTable("AAAn");
    }

    void createSimple(String a, Object[][] ver) throws SQLException, IOException {
        try (Statement st = ucanaccess.createStatement()) {
            st.execute("INSERT INTO AAAn VALUES ('33A',11,'" + a + "'   )");
            st.execute("INSERT INTO AAAn VALUES ('33B',111,'" + a + "'    )");
            checkQuery("SELECT * FROM AAAn", ver);
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.test.util.AccessVersion#getDefaultAccessVersion()")
    void testDrop(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);
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
