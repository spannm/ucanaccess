package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

class AccessLikeTest extends UcanaccessBaseTest {

    @Override
    protected String getAccessPath() {
        return getTestDbDir() + "accessLike.mdb"; // Access 2000
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.type.AccessVersion#getDefaultAccessVersion()")
    void testLike(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);
        checkQuery("SELECT * FROM query1 ORDER BY campo2", singleRec("dd1"));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.type.AccessVersion#getDefaultAccessVersion()")
    void testLikeExternal(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);

        String tableName = "T21";
        try (Statement st = ucanaccess.createStatement()) {
            st.executeUpdate("CREATE TABLE " + tableName + " (id COUNTER PRIMARY KEY, descr MEMO)");

            for (String val : List.of(
                "dsdsds", "aa", "aBa", "aBBBa", "PB123", "PZ123", "a*a", "A*a", "ss#sss", "*", "132B", "138", "138#")) {
                st.execute("INSERT INTO T21 (descr) VALUES('" + val + "')");
            }
            
            checkQuery("SELECT descr FROM T21 WHERE descr LIKE 'a[*]a' ORDER BY ID",
                recs(rec("a*a"), rec("A*a")));

            checkQuery("SELECT descr FROM T21 WHERE descr LIKE \"a*a\" AND '1'='1' AND (descr) like \"a*a\" ORDER BY ID",
                recs(rec("aa"), rec("aBa"), rec("aBBBa"), rec("a*a"), rec("A*a")));

            checkQuery("SELECT * FROM T21 WHERE descr LIKE 'a%a'",
                recs(rec(2, "aa"), rec(3, "aBa"), rec(4, "aBBBa"), rec(7, "a*a"), rec(8, "A*a")));
            checkQuery("SELECT descr FROM T21 WHERE descr LIKE 'P[A-F]###'", singleRec("PB123"));
            checkQuery("SELECT descr FROM T21 WHERE (T21.descr\n) \nLIKE 'P[!A-F]###' AND '1'='1'", singleRec("PZ123"));
            checkQuery("SELECT * FROM T21 WHERE descr='aba'", singleRec(3, "aBa"));
            checkQuery("SELECT descr FROM T21 WHERE descr LIKE '13[1-4][A-F]'", singleRec("132B"));
            checkQuery("SELECT descr FROM T21 WHERE descr LIKE '13[!1-4]'", singleRec("138"));
            checkQuery("SELECT descr FROM T21 WHERE descr LIKE '%s[#]%'", singleRec("ss#sss"));
            checkQuery("SELECT descr FROM T21 WHERE descr LIKE '###'", singleRec("138"));
            checkQuery("SELECT descr FROM T21 WHERE descr LIKE '###[#]'", singleRec("138#"));
            checkQuery("SELECT descr FROM T21 WHERE descr LIKE '###[#]'", singleRec("138#"));
            checkQuery("SELECT descr FROM T21 WHERE ((descr LIKE '###[#]'))", singleRec("138#"));
            st.execute("DROP TABLE " + tableName);
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.type.AccessVersion#getDefaultAccessVersion()")
    void testNotLikeExternal(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        String tableName = "Tx21";
        try (Statement st = ucanaccess.createStatement()) {
            st.executeUpdate("CREATE TABLE " + tableName + " (id COUNTER PRIMARY KEY, descr MEMO)");

            st.execute("INSERT INTO Tx21(descr) VALUES('t11114')");
            st.execute("INSERT INTO Tx21(descr) VALUES('t1111C')");
            st.execute("INSERT INTO Tx21(descr) VALUES('t1111')");
            checkQuery("SELECT DESCR FROM Tx21 WHERE descr NOT LIKE \"t#####\" ORDER BY id",
                recs(rec("t1111C"), rec("t1111")));
            st.execute("DROP TABLE " + tableName);
        }
    }
}
