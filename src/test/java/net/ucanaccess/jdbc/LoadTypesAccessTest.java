package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

class LoadTypesAccessTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements(
            "CREATE TABLE t_types (id LONG, descr MEMO, dt DATETIME, euros CURRENCY, float1 SINGLE, double1 DOUBLE, int1 INTEGER, numeric0 NUMERIC(24,5), numeric1 DOUBLE)",
            "INSERT INTO t_types (id, descr, dt, euros, float1, double1, int1, numeric0, numeric1 ) "
                + "VALUES(1234, 'I like trippa with spaghettis bolognese', #10/03/2008 10:34:35 PM#, 4.55555, 5.6666, 6.7, 5, 0.100051, 4.677856)");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testDate(AccessVersion _accessVersion) throws SQLException, ParseException {
        init(_accessVersion);
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        checkQuery("SELECT #10/03/2004#, #11/23/1811#, #10/03/2008 22:34:35#, #10/03/2008 22:34:35 aM#, #10/03/2008 10:34:35 PM# FROM t_types",
            recs(rec(df.parse("2004-10-03 00:00:00.0"), df.parse("1811-11-23 00:00:00.0"), df.parse("2008-10-03 22:34:35.0"), df.parse("2008-10-03 22:34:35.0"), df.parse("2008-10-03 22:34:35.0"))));
        checkQuery("SELECT #22:34:35#,#10:34:35 AM#,#10:34:35 pM# FROM t_types",
            recs(rec(df.parse("1899-12-30 22:34:35.0"), df.parse("1899-12-30 10:34:35.0"), df.parse("1899-12-30 22:34:35.0"))));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testQuery(AccessVersion _accessVersion) throws SQLException, ParseException {
        init(_accessVersion);
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        checkQuery("SELECT * FROM t_types",
            recs(rec(1234, "I like trippa with spaghettis bolognese", df.parse("2008-10-03 22:34:35"), 4.5555, 5.6666, 6.7, 5, 0.10005, 4.677856)));
    }
}
