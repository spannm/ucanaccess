package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessTestBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;

class DropTableTest extends UcanaccessTestBase {

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE AAAn ( baaaa TEXT(3) PRIMARY KEY,A INTEGER , C TEXT(4)) ",
            "CREATE TABLE [AAA n] ( baaaa TEXT(3) PRIMARY KEY,A INTEGER , C TEXT(4)) ");
    }

    void createSimple(String _tableName, String a, Object[][] ver) throws SQLException, IOException {
        try (Statement st = ucanaccess.createStatement()) {
            st.execute("INSERT INTO " + _tableName + " VALUES ('33A',11,'" + a + "'   )");
            st.execute("INSERT INTO " + _tableName + " VALUES ('33B',111,'" + a + "'    )");
            checkQuery("SELECT * FROM " + _tableName + " ORDER BY c", ver);
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testDrop(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);

        // ucanaccess.setAutoCommit(false);
        createSimple("AAAn", "a", new Object[][] {{"33A", 11, "a"}, {"33B", 111, "a"}});
        try (Statement st = ucanaccess.createStatement()) {
            st.executeUpdate("DROP TABLE AAAn");
            // ucanaccess.commit();
            st.execute("CREATE TABLE AAAn ( baaaa TEXT(3) PRIMARY KEY,A INTEGER , C TEXT(4)) ");
            createSimple("AAAn", "b", new Object[][] {{"33A", 11, "b"}, {"33B", 111, "b"}});
            dumpQueryResult("SELECT * FROM AAAn");
            ucanaccess.commit();
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testDropBlank(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);

        // ucanaccess.setAutoCommit(false);
        createSimple("[AAA n]", "a", new Object[][] {{"33A", 11, "a"}, {"33B", 111, "a"}});
        try (Statement st = ucanaccess.createStatement()) {
            st.executeUpdate("DROP TABLE [AAA n]");
            // ucanaccess.commit();
            st.execute("CREATE TABLE [AAA n] ( baaaa TEXT(3) PRIMARY KEY,A INTEGER , C TEXT(4)) ");
            createSimple("[AAA n]", "b", new Object[][] {{"33A", 11, "b"}, {"33B", 111, "b"}});
            dumpQueryResult("SELECT * FROM [AAA n]");
            ucanaccess.commit();
        }
    }

}
