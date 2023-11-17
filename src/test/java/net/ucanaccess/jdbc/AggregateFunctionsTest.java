package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Locale;

class AggregateFunctionsTest extends UcanaccessBaseTest {

    AggregateFunctionsTest() {
        Locale.setDefault(Locale.US);
    }

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);

        executeStatements("CREATE TABLE t235 (id INTEGER, descr TEXT(400), num NUMERIC(12,3), date0 DATETIME)",
            "INSERT INTO t235 (id, descr, num, date0) VALUES(1234, 'Show must go off',-1110.55446,#11/22/2003 10:42:58 PM#)",
            "INSERT INTO t235 (id, descr, num, date0) VALUES(12344, 'Show must go up and down',-113.55446,#11/22/2006 10:42:58 PM#)");
    }

    @AfterEach
    void afterEachTest() throws SQLException {
        executeStatements("DROP TABLE t235");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testDCount(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);

        checkQuery("SELECT id, DCount('*', 't235', '1=1') FROM [t235]", recs(rec(1234, 2), rec(12344, 2)));
        checkQuery("SELECT id AS [WW \"SS], DCount('descr', 't235', '1=1') FROM t235",
            recs(rec(1234, 2), rec(12344, 2)));
        checkQuery("SELECT DCount('*', 't235', '1=1') ", singleRec(2));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testDSum(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);
        checkQuery("SELECT DSum('id', 't235', '1=1')", singleRec(13578));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testDMax(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);
        checkQuery("SELECT DMax('id', 't235')", singleRec(12344));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testDMin(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);
        checkQuery("SELECT DMin('id', 't235')", singleRec(1234));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testDAvg(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);
        checkQuery("SELECT DAvg('id', 't235')", singleRec(6789));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testLast(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);
        checkQuery("SELECT last(descr) FROM t235", singleRec("Show must go up and down"));
        checkQuery("SELECT last(NUM) FROM t235", singleRec(-113.5540));
        dumpQueryResult("SELECT last(date0) FROM t235");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testFirst(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);
        checkQuery("SELECT first(descr) FROM t235", singleRec("Show must go off"));
        checkQuery("SELECT first(NUM) FROM t235", singleRec(-1110.5540));
        dumpQueryResult("SELECT first(date0) FROM t235");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testDLast(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);
        checkQuery("SELECT DLast('descr', 't235')", singleRec("Show must go up and down"));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testDFirst(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);
        checkQuery("SELECT DFirst('descr', 't235') ", singleRec("Show must go off"));
    }

}
