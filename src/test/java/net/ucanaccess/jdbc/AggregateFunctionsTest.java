package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.SQLException;

class AggregateFunctionsTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion accessVersion) throws SQLException {
        super.init(accessVersion);

        executeStatements(
            "CREATE TABLE t_aggrfunc (id INTEGER, descr TEXT(400), num NUMERIC(12,3), date0 DATETIME)",
            "INSERT INTO t_aggrfunc (id, descr, num, date0) VALUES(1234, 'Show must go off', -1110.55446, #11/22/2003 10:42:58 PM#)",
            "INSERT INTO t_aggrfunc (id, descr, num, date0) VALUES(12344, 'Show must go up and down', -113.55446, #11/22/2006 10:42:58 PM#)");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testDCount(AccessVersion accessVersion) throws SQLException {
        init(accessVersion);

        checkQuery("SELECT id, DCount('*', 't_aggrfunc', '1=1') FROM [t_aggrfunc]", recs(rec(1234, 2), rec(12344, 2)));
        checkQuery("SELECT id AS [WW \"SS], DCount('descr', 't_aggrfunc', '1=1') FROM t_aggrfunc", recs(rec(1234, 2), rec(12344, 2)));
        checkQuery("SELECT DCount('*', 't_aggrfunc', '1=1') ", singleRec(2));
    }

    // a criterion built with & refers to the outer row
    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testDynamicCriteria(AccessVersion accessVersion) throws SQLException {
        init(accessVersion);

        checkQuery("SELECT id, DCount('*', 't_aggrfunc', 'id <= ' & id) FROM t_aggrfunc ORDER BY id", recs(rec(1234, 1), rec(12344, 2)));
        checkQuery("SELECT id, DLookup('descr', 't_aggrfunc', 'id = ' & id) FROM t_aggrfunc ORDER BY id",
            recs(rec(1234, "Show must go off"), rec(12344, "Show must go up and down")));
        checkQuery("SELECT id, DLookup(\"id\", \"t_aggrfunc\", \"descr='\" & descr & \"'\") FROM t_aggrfunc ORDER BY id",
            recs(rec(1234, 1234), rec(12344, 12344)));
    }

    // the arguments end at their own parenthesis, and quotes in a criterion are doubled inside the literal
    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testSeveralDomainFunctionsAndQuotedCriteria(AccessVersion accessVersion) throws SQLException {
        init(accessVersion);

        checkQuery("SELECT DSum('id', 't_aggrfunc'), DAvg('id', 't_aggrfunc'), DCount('*', 't_aggrfunc', 'id > 2000')",
            singleRec(13578, 6789, 1));
        checkQuery("SELECT DLookup(\"id\", \"t_aggrfunc\", \"descr='Show must go off'\")", singleRec(1234));
        checkQuery("SELECT DCount('*', 't_aggrfunc', 'descr IN (''Show must go off'', ''x'')')", singleRec(1));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testDSum(AccessVersion accessVersion) throws SQLException {
        init(accessVersion);
        checkQuery("SELECT DSum('id', 't_aggrfunc', '1=1')", singleRec(13578));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testDMax(AccessVersion accessVersion) throws SQLException {
        init(accessVersion);
        checkQuery("SELECT DMax('id', 't_aggrfunc')", singleRec(12344));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testDMin(AccessVersion accessVersion) throws SQLException {
        init(accessVersion);
        checkQuery("SELECT DMin('id', 't_aggrfunc')", singleRec(1234));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testDAvg(AccessVersion accessVersion) throws SQLException {
        init(accessVersion);
        checkQuery("SELECT DAvg('id', 't_aggrfunc')", singleRec(6789));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testLast(AccessVersion accessVersion) throws SQLException {
        init(accessVersion);
        checkQuery("SELECT last(descr) FROM t_aggrfunc", singleRec("Show must go up and down"));
        checkQuery("SELECT last(NUM) FROM t_aggrfunc", singleRec(-113.5540));
        dumpQueryResult("SELECT last(date0) FROM t_aggrfunc");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testFirst(AccessVersion accessVersion) throws SQLException {
        init(accessVersion);
        checkQuery("SELECT first(descr) FROM t_aggrfunc", singleRec("Show must go off"));
        checkQuery("SELECT first(NUM) FROM t_aggrfunc", singleRec(-1110.5540));
        dumpQueryResult("SELECT first(date0) FROM t_aggrfunc");
    }

    // First and Last of number columns used to return a boolean or fail, as no overload matched
    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testFirstLastOfNumbers(AccessVersion accessVersion) throws SQLException {
        init(accessVersion);
        executeStatements(
            "CREATE TABLE t_firstlast (l LONG, b BYTE, i INTEGER, d DOUBLE, s SINGLE)",
            "INSERT INTO t_firstlast VALUES (3, 4, 5, 1.5, 2.5)",
            "INSERT INTO t_firstlast VALUES (8, 9, 10, 6.5, 7.5)");
        checkQuery("SELECT First(l), First(b), First(i), First(d), First(s) FROM t_firstlast", singleRec(3, 4, 5, 1.5, 2.5));
        checkQuery("SELECT Last(l), Last(b), Last(i), Last(d), Last(s) FROM t_firstlast", singleRec(8, 9, 10, 6.5, 7.5));
        checkQuery("SELECT first(id), last(id) FROM t_aggrfunc", singleRec(1234, 12344));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testDLast(AccessVersion accessVersion) throws SQLException {
        init(accessVersion);
        checkQuery("SELECT DLast('descr', 't_aggrfunc')", singleRec("Show must go up and down"));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testDFirst(AccessVersion accessVersion) throws SQLException {
        init(accessVersion);
        checkQuery("SELECT DFirst('descr', 't_aggrfunc') ", singleRec("Show must go off"));
    }

    // sample and population standard deviation and variance, (1234, 12344) differ by 11110
    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testDStDevDVar(AccessVersion accessVersion) throws SQLException {
        init(accessVersion);
        checkQuery("SELECT DStDev('id', 't_aggrfunc')", singleRec(11110 / Math.sqrt(2)));
        checkQuery("SELECT DStDevP('id', 't_aggrfunc')", singleRec(5555.0));
        checkQuery("SELECT DVar('id', 't_aggrfunc')", singleRec(11110.0 * 11110 / 2));
        checkQuery("SELECT DVarP('id', 't_aggrfunc', 'id > 0')", singleRec(5555.0 * 5555));
    }

}
