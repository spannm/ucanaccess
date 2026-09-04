package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.SQLException;
import java.util.List;

class DropTableTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion accessVersion) throws SQLException {
        super.init(accessVersion);
        executeStatements("CREATE TABLE AAAn (baaaa TEXT(3) PRIMARY KEY, A INTEGER, C TEXT(4))",
            "CREATE TABLE [AAA n] (baaaa TEXT(3) PRIMARY KEY, A INTEGER, C TEXT(4))");
    }

    void createSimple(String tableName, String a, List<List<Object>> ver) throws SQLException {
        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.execute("INSERT INTO " + tableName + " VALUES ('33A', 11, '" + a + "')");
            st.execute("INSERT INTO " + tableName + " VALUES ('33B', 111, '" + a + "')");
            checkQuery("SELECT * FROM " + tableName + " ORDER BY c", ver);
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testDrop(AccessVersion accessVersion) throws SQLException {
        init(accessVersion);

        // ucanaccess.setAutoCommit(false);
        createSimple("AAAn", "a", recs(rec("33A", 11, "a"), rec("33B", 111, "a")));
        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.executeUpdate("DROP TABLE AAAn");
            // ucanaccess.commit();
            st.execute("CREATE TABLE AAAn (baaaa TEXT(3) PRIMARY KEY, A INTEGER, C TEXT(4))");
            createSimple("AAAn", "b", recs(rec("33A", 11, "b"), rec("33B", 111, "b")));
            dumpQueryResult("SELECT * FROM AAAn");
            ucanaccess.commit();
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testDropBlank(AccessVersion accessVersion) throws SQLException {
        init(accessVersion);

        // ucanaccess.setAutoCommit(false);
        createSimple("[AAA n]", "a", recs(rec("33A", 11, "a"), rec("33B", 111, "a")));
        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.executeUpdate("DROP TABLE [AAA n]");
            // ucanaccess.commit();
            st.execute("CREATE TABLE [AAA n] (baaaa TEXT(3) PRIMARY KEY, A INTEGER, C TEXT(4))");
            createSimple("[AAA n]", "b", recs(rec("33A", 11, "b"), rec("33B", 111, "b")));
            dumpQueryResult("SELECT * FROM [AAA n]");
            ucanaccess.commit();
        }
    }

}
