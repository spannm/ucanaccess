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

}
