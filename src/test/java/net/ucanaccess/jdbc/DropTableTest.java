package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

class DropTableTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE AAAn (baaaa TEXT(3) PRIMARY KEY, A INTEGER, C TEXT(4))",
            "CREATE TABLE [AAA n] (baaaa TEXT(3) PRIMARY KEY, A INTEGER, C TEXT(4))");
    }

    void createSimple(String _tableName, String _a, List<List<Object>> _ver) throws SQLException {
        try (Statement st = ucanaccess.createStatement()) {
            st.execute("INSERT INTO " + _tableName + " VALUES ('33A', 11,'" + _a + "')");
            st.execute("INSERT INTO " + _tableName + " VALUES ('33B',111,'" + _a + "')");
            checkQuery("SELECT * FROM " + _tableName + " ORDER BY c", _ver);
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testDrop(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        // ucanaccess.setAutoCommit(false);
        createSimple("AAAn", "a", recs(rec("33A", 11, "a"), rec("33B", 111, "a")));
        try (Statement st = ucanaccess.createStatement()) {
            st.executeUpdate("DROP TABLE AAAn");
            // ucanaccess.commit();
            st.execute("CREATE TABLE AAAn (baaaa TEXT(3) PRIMARY KEY, A INTEGER, C TEXT(4))");
            createSimple("AAAn", "b", recs(rec("33A", 11, "b"), rec("33B", 111, "b")));
            dumpQueryResult("SELECT * FROM AAAn");
            ucanaccess.commit();
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testDropBlank(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        // ucanaccess.setAutoCommit(false);
        createSimple("[AAA n]", "a", recs(rec("33A", 11, "a"), rec("33B", 111, "a")));
        try (Statement st = ucanaccess.createStatement()) {
            st.executeUpdate("DROP TABLE [AAA n]");
            // ucanaccess.commit();
            st.execute("CREATE TABLE [AAA n] (baaaa TEXT(3) PRIMARY KEY, A INTEGER, C TEXT(4))");
            createSimple("[AAA n]", "b", recs(rec("33A", 11, "b"), rec("33B", 111, "b")));
            dumpQueryResult("SELECT * FROM [AAA n]");
            ucanaccess.commit();
        }
    }

}
