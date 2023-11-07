package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessVersion;
import net.ucanaccess.test.UcanaccessBaseTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;

class MetaDataTest extends UcanaccessBaseTest {

    @Override
    protected String getAccessPath() {
        return TEST_DB_DIR + "noroman.mdb";
    }

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE t_metadata ( baaaa TEXT(3) PRIMARY KEY, A INTEGER, C TEXT(4))");
    }

    @AfterEach
    void afterEachTest() throws SQLException {
        dropTable("t_metadata");
    }

    void createSimple(String a, Object[][] ver) throws SQLException {
        try (Statement st = ucanaccess.createStatement()) {
            st.execute("INSERT INTO t_metadata VALUES ('33A', 11, '" + a + "' )");
            st.execute("INSERT INTO t_metadata VALUES ('33B', 111, '" + a + "' )");
        }
        checkQuery("SELECT * FROM t_metadata", ver);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.test.AccessVersion#getDefaultAccessVersion()")
    void testDrop(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);
        ucanaccess.setAutoCommit(false);
        createSimple("a", new Object[][] {{"33A", 11, "a"}, {"33B", 111, "a"}});
        try (Statement st = ucanaccess.createStatement()) {
            st.executeUpdate("DROP TABLE t_metadata");

            st.execute("CREATE TABLE t_metadata (baaaa TEXT(3) PRIMARY KEY, A INTEGER, C TEXT(4))");
            createSimple("b", new Object[][] {{"33A", 11, "b"}, {"33B", 111, "b"}});

            ucanaccess.commit();
        }
    }
}
