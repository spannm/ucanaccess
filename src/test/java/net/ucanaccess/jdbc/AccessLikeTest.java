package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessVersion;
import net.ucanaccess.test.UcanaccessBaseTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

class AccessLikeTest extends UcanaccessBaseTest {

    @Override
    protected String getAccessPath() {
        return TEST_DB_DIR + "accessLike.mdb"; // Access 2000
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.test.AccessVersion#getDefaultAccessVersion()")
    void testLike(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);
        checkQuery("SELECT * FROM query1 ORDER BY campo2", "dd1");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.test.AccessVersion#getDefaultAccessVersion()")
    void testLikeExternal(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);

        String tableName = "T21";
        try (Statement st = ucanaccess.createStatement()) {
            st.executeUpdate("CREATE TABLE " + tableName + " (id COUNTER PRIMARY KEY, descr MEMO)");

            for (String val : List.of(
                "dsdsds", "aa", "aBa", "aBBBa", "PB123", "PZ123", "a*a", "A*a", "ss#sss", "*", "132B", "138", "138#")) {
                st.execute("INSERT INTO T21 (descr) VALUES('" + val + "')");
            }
            Object[][] ver = {{"a*a"}, {"A*a"}};
            checkQuery("SELECT descr FROM T21 WHERE descr LIKE 'a[*]a' ORDER BY ID", ver);

            ver = new Object[][] {{"aa"}, {"aBa"}, {"aBBBa"}, {"a*a"}, {"A*a"}};
            checkQuery("SELECT descr FROM T21 WHERE descr LIKE \"a*a\" AND '1'='1' AND (descr) like \"a*a\" ORDER BY ID", ver);

            ver = new Object[][] {{2, "aa"}, {3, "aBa"}, {4, "aBBBa"}, {7, "a*a"}, {8, "A*a"}};
            checkQuery("SELECT * FROM T21 WHERE descr LIKE 'a%a'", ver);

            checkQuery("SELECT descr FROM T21 WHERE descr LIKE 'P[A-F]###'", "PB123");
            checkQuery("SELECT descr FROM T21 WHERE (T21.descr\n) \nLIKE 'P[!A-F]###' AND '1'='1'", "PZ123");
            checkQuery("SELECT * FROM T21 WHERE descr='aba'", 3, "aBa");
            checkQuery("SELECT descr FROM T21 WHERE descr LIKE '13[1-4][A-F]'", "132B");
            checkQuery("SELECT descr FROM T21 WHERE descr LIKE '13[!1-4]'", "138");
            checkQuery("SELECT descr FROM T21 WHERE descr LIKE '%s[#]%'", "ss#sss");
            checkQuery("SELECT descr FROM T21 WHERE descr LIKE '###'", "138");
            checkQuery("SELECT descr FROM T21 WHERE descr LIKE '###[#]'", "138#");

            checkQuery("SELECT descr FROM T21 WHERE descr LIKE '###[#]'", "138#");
            checkQuery("SELECT descr FROM T21 WHERE ((descr LIKE '###[#]'))", "138#");
            st.execute("DROP TABLE " + tableName);
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.test.AccessVersion#getDefaultAccessVersion()")
    void testNotLikeExternal(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        String tableName = "Tx21";
        try (Statement st = ucanaccess.createStatement()) {
            st.executeUpdate("CREATE TABLE " + tableName + " (id COUNTER PRIMARY KEY, descr MEMO)");

            st.execute("INSERT INTO Tx21(descr) VALUES('t11114')");
            st.execute("INSERT INTO Tx21(descr) VALUES('t1111C')");
            st.execute("INSERT INTO Tx21(descr) VALUES('t1111')");
            checkQuery("SELECT DESCR FROM Tx21 WHERE descr NOT LIKE \"t#####\" ORDER BY id", new Object[][] {{"t1111C"}, {"t1111"}});
            st.execute("DROP TABLE " + tableName);
        }
    }
}
