package net.ucanaccess.jdbc;

import static net.ucanaccess.type.SqlConstants.*;

import net.ucanaccess.test.AccessDefaultVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import net.ucanaccess.util.Sql;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.SQLException;
import java.util.List;

class AccessLikeTest extends UcanaccessBaseTest {

    @Override
    protected String getAccessPath() {
        return getTestDbDir() + "accessLike.mdb"; // Access 2000
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessDefaultVersionSource
    void testLike(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);
        checkQuery(Sql.of(SELECT, "*", FROM, "q_like2", ORDER_BY, "campo2"), singleRec("dd1"));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessDefaultVersionSource
    void testLikeExternal(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.executeUpdate("CREATE TABLE t_like3 (id COUNTER PRIMARY KEY, descr MEMO)");

            for (String val : List.of(
                "dsdsds", "aa", "aBa", "aBBBa", "PB123", "PZ123", "a*a", "A*a", "ss#sss", "*", "132B", "138", "138#")) {
                st.execute("INSERT INTO t_like3 (descr) VALUES('" + val + "')");
            }

            checkQuery(Sql.of(SELECT, "descr", FROM, "t_like3", WHERE, "descr", LIKE, "'a[*]a'", ORDER_BY, "ID"),
                recs(rec("a*a"), rec("A*a")));

            checkQuery(Sql.of(SELECT, "descr", FROM, "t_like3", WHERE, "descr", LIKE, "\"a*a\" AND '1'='1' AND (descr) like \"a*a\"", ORDER_BY, "ID"),
                recs(rec("aa"), rec("aBa"), rec("aBBBa"), rec("a*a"), rec("A*a")));

            checkQuery(Sql.of(SELECT, "*", FROM, "t_like3", WHERE, "descr", LIKE, "'a%a'"),
                recs(rec(2, "aa"), rec(3, "aBa"), rec(4, "aBBBa"), rec(7, "a*a"), rec(8, "A*a")));
            checkQuery(Sql.of(SELECT, "descr", FROM, "t_like3", WHERE, "descr", LIKE, "'P[A-F]###'"), singleRec("PB123"));
            checkQuery(Sql.of(SELECT, "descr", FROM, "t_like3", WHERE, "(t_like3.descr\n) \n", LIKE, "'P[!A-F]###' AND '1'='1'"), singleRec("PZ123"));
            checkQuery(Sql.of(SELECT, "*", FROM, "t_like3", WHERE, "descr='aba'"), singleRec(3, "aBa"));
            checkQuery(Sql.of(SELECT, "descr", FROM, "t_like3", WHERE, "descr", LIKE, "'13[1-4][A-F]'"), singleRec("132B"));
            checkQuery(Sql.of(SELECT, "descr", FROM, "t_like3", WHERE, "descr", LIKE, "'13[!1-4]'"), singleRec("138"));
            checkQuery(Sql.of(SELECT, "descr", FROM, "t_like3", WHERE, "descr", LIKE, "'%s[#]%'"), singleRec("ss#sss"));
            checkQuery(Sql.of(SELECT, "descr", FROM, "t_like3", WHERE, "descr", LIKE, "'###'"), singleRec("138"));
            checkQuery(Sql.of(SELECT, "descr", FROM, "t_like3", WHERE, "descr", LIKE, "'###[#]'"), singleRec("138#"));
            checkQuery(Sql.of(SELECT, "descr", FROM, "t_like3", WHERE, "descr", LIKE, "'###[#]'"), singleRec("138#"));
            checkQuery(Sql.of(SELECT, "descr", FROM, "t_like3", WHERE, "descr", LIKE, "'###[#]'"), singleRec("138#"));
            st.execute("DROP TABLE t_like3");
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessDefaultVersionSource
    void testNotLikeExternal(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.executeUpdate("CREATE TABLE " + "t_like4" + " (id COUNTER PRIMARY KEY, descr MEMO)");

            st.execute("INSERT INTO t_like4(descr) VALUES('t11114')");
            st.execute("INSERT INTO t_like4(descr) VALUES('t1111C')");
            st.execute("INSERT INTO t_like4(descr) VALUES('t1111')");
            checkQuery(Sql.of(SELECT, "DESCR", FROM, "t_like4", WHERE, "descr NOT", LIKE, "\"t#####\"", ORDER_BY, "id"),
                recs(rec("t1111C"), rec("t1111")));
            st.execute("DROP TABLE t_like4");
        }
    }
}
