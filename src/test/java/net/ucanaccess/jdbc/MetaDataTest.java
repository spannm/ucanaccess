package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessDefaultVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.SQLException;
import java.util.List;

class MetaDataTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion accessVersion) throws SQLException {
        super.init(accessVersion);
        executeStatements("CREATE TABLE t_metadata ( baaaa TEXT(3) PRIMARY KEY, A INTEGER, C TEXT(4))");
    }

    void insertData(String a, List<List<Object>> ver) throws SQLException {
        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.execute("INSERT INTO t_metadata VALUES ('33A', 11, '" + a + "')");
            st.execute("INSERT INTO t_metadata VALUES ('33B', 111, '" + a + "')");
        }
        checkQuery("SELECT * FROM t_metadata", ver);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessDefaultVersionSource
    void testDrop(AccessVersion accessVersion) throws SQLException {
        init(accessVersion);
        ucanaccess.setAutoCommit(false);
        insertData("a", recs(rec("33A", 11, "a"), rec("33B", 111, "a")));
        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.executeUpdate("DROP TABLE t_metadata");

            st.execute("CREATE TABLE t_metadata (baaaa TEXT(3) PRIMARY KEY, A INTEGER, C TEXT(4))");
            insertData("b", recs(rec("33A", 11, "b"), rec("33B", 111, "b")));

            ucanaccess.commit();
        }
    }
}
