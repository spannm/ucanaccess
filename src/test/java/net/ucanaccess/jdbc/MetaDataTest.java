package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

class MetaDataTest extends UcanaccessBaseTest {

    @Override
    protected String getAccessPath() {
        return getTestDbDir() + "noroman.mdb";
    }

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE t_metadata ( baaaa TEXT(3) PRIMARY KEY, A INTEGER, C TEXT(4))");
    }

    void createSimple(String _a, List<List<Object>> _ver) throws SQLException {
        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.execute("INSERT INTO t_metadata VALUES ('33A', 11, '" + _a + "' )");
            st.execute("INSERT INTO t_metadata VALUES ('33B', 111, '" + _a + "' )");
        }
        checkQuery("SELECT * FROM t_metadata", _ver);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.type.AccessVersion#getDefaultAccessVersion()")
    void testDrop(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);
        ucanaccess.setAutoCommit(false);
        createSimple("a", recs(rec("33A", 11, "a"), rec("33B", 111, "a")));
        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.executeUpdate("DROP TABLE t_metadata");

            st.execute("CREATE TABLE t_metadata (baaaa TEXT(3) PRIMARY KEY, A INTEGER, C TEXT(4))");
            createSimple("b", recs(rec("33A", 11, "b"), rec("33B", 111, "b")));

            ucanaccess.commit();
        }
    }
}
