package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessBaseTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

class FunctionsTest extends UcanaccessBaseTest {

    FunctionsTest() {
        Locale.setDefault(Locale.US);
    }

    @Override
    protected String getAccessPath() {
        return TEST_DB_DIR + "functions" + getFileFormat().name() + getFileFormat().getFileExtension();
    }

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements(
            "CREATE TABLE tblFormat (id int NOT NULL PRIMARY KEY, text TEXT, date DATETIME, number NUMERIC);",
            "INSERT INTO tblFormat (id) VALUES(1)");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testASC(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT ASC('A') FROM t234", 65);
        checkQuery("SELECT ASC('1') FROM t234", 49);
        checkQuery("SELECT ASC('u') FROM t234", 117);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testSwitch(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT SWITCH('1'='1', 1, false, 2, true, 1) FROM t234");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testATN(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT ATN(3) FROM t234", 1.2490457723982544);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testNz(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT NZ(null,'lampredotto'),nz('turtelaz','lampredotto'), nz(null, 1.5), nz(2, 2) FROM t234",
            "lampredotto", "turtelaz", 1.5, 2);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testCBoolean(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT CBOOL(id), CBOOL(1=2), CBOOL('true'), CBOOL('false'), CBOOL(0), CBOOL(-3) FROM t234",
            new Object[][] {{true, false, true, false, false, true}});
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testCVar(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT CVAR(8), CVAR(8.44) FROM t234", "8", "8.44");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testCstr(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT CSTR(date0) FROM t234", "11/22/2003 10:42:58 PM");
        checkQuery("SELECT CSTR(false) FROM t234", "false");
        checkQuery("SELECT CSTR(8) FROM t234", "8");
        checkQuery("SELECT CSTR(8.78787878) FROM t234", "8.78787878");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testCsign(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT CSIGN(8.53453543) FROM t234", 8.534535);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testCDate(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

        checkQuery("SELECT CDATE('Apr 6, 2003') FROM t234", sdf.parse("2003-04-06 00:00:00.0"));

        checkQuery("SELECT CDATE('1582-10-15') FROM t234", sdf.parse("1582-10-15 00:00:00.0"));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testCLong(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT CLONG(8.52), CLONG(8.49), CLONG(5.5) FROM t234", 9, 8, 6);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testCLng(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT CLNG(8.52), CLNG(8.49), CLNG(5.5) FROM t234", 9, 8, 6);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testCDec(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT CDEC(8.45 * 0.005 * 0.01) FROM t234", 0.0004225);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testCcur(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT CCUR(123.4567812), CCUR(123.4547812) FROM t234", 123.4568, 123.4548);

        checkQuery("SELECT CCUR(0.552222211) * 100 FROM t234", 55.22);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testCint(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT CINT(8.51), CINT(4.5) FROM t234", 9, 4);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testChr(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT CHR(65) FROM t234", "A");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testCos(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT COS(1) FROM t234", 0.5403023058681398);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testCurrentUser(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT CurrentUser() FROM t234", "ucanaccess");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testDateAdd(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

        checkQuery("SELECT DateAdd('YYYY', 4, #11/22/2003 10:42:58 PM#) FROM t234",
            sdf.parse("2007-11-22 22:42:58"));
        checkQuery("SELECT DateAdd('Q', 3 ,#11/22/2003 10:42:58 PM#) FROM t234",
            sdf.parse("2004-08-22 22:42:58"));
        checkQuery("SELECT DateAdd('Y', 451 ,#11/22/2003 10:42:58 PM#) FROM t234",
            sdf.parse("2005-02-15 22:42:58"));
        checkQuery("SELECT DateAdd('D', 451 ,#11/22/2003 10:42:58 PM#) FROM t234",
            sdf.parse("2005-02-15 22:42:58"));
        checkQuery("SELECT DateAdd('Y', 45 ,#11/22/2003 10:42:58 PM#) FROM t234",
            sdf.parse("2004-01-06 22:42:58"));
        checkQuery("SELECT DateAdd('D', 45 ,#11/22/2003 10:42:58 PM#) FROM t234",
            sdf.parse("2004-01-06 22:42:58"));
        checkQuery("SELECT DateAdd('Y', 4 ,#11/22/2003 10:42:58 PM#) FROM t234",
            sdf.parse("2003-11-26 22:42:58"));
        checkQuery("SELECT DateAdd('D', 4 ,#11/22/2003 10:42:58 PM#) FROM t234",
            sdf.parse("2003-11-26 22:42:58"));
        checkQuery("SELECT DateAdd('W', 43 ,#11/22/2003 10:42:58 PM#) FROM t234",
            sdf.parse("2004-01-04 22:42:58"));
        checkQuery("SELECT DateAdd('W', 1 ,#11/22/2003 10:42:58 PM#) FROM t234",
            sdf.parse("2003-11-23 22:42:58"));
        checkQuery("SELECT DateAdd('WW', 43 ,#11/22/2003 10:42:58 PM#) FROM t234",
            sdf.parse("2004-09-18 22:42:58"));
        checkQuery("SELECT DateAdd('H', 400 ,#11/22/2003 10:42:58 PM#) FROM t234",
            sdf.parse("2003-12-09 14:42:58"));
        checkQuery("SELECT DateAdd('M', 400 ,#11/22/2003 10:42:58 PM#) FROM t234",
            sdf.parse("2037-03-22 22:42:58"));
        checkQuery("SELECT DateAdd('S', 400 ,#11/22/2003 10:42:58 PM#) FROM t234",
            sdf.parse("2003-11-22 22:49:38"));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testDate(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Date() FROM t234");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testDay(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Day(#11/22/2003 10:42:58 PM#) FROM t234", 22);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testExp(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Exp(3.1), exp(0.4) FROM t234", 22.197951281441636, 1.4918246976412703);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testHour(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Hour(#10:42:58 pM#), Hour(#10:42:58 AM#), Hour(#11/22/2003 10:42:58 PM#) FROM t234", 22, 10,
            22);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testIif(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery(
            "SELECT IIf(IsNull(descr)=true, 'pippo', 'pl''uto'&'\" \" cccc'), IIf(IsNull(descr)=true,'pippo','pl''uto'&'\" \" cccc') FROM t234",
            "pl'uto\" \" cccc", "pl'uto\" \" cccc");

        checkQuery("SELECT IIf(true, false, true) FROM t234", false);
        checkQuery("SELECT IIf('pippo'=null, 'capra', 'd''una capra') FROM t234", "d'una capra");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testInstr(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT InStr('Found on the Net', 'the') FROM t234", 10);
        checkQuery("SELECT InStr('Found on the Net', 'f') FROM t234", 1);
        checkQuery("SELECT InStr(1, 'Found on the Net', 'f') FROM t234", 1);
        checkQuery("SELECT InStr(1, 'Found on the Net', 'f',1) FROM t234", 1);
        checkQuery("SELECT InStr(1, 'Found on the Net', 't',0) FROM t234", 10);
        checkQuery("SELECT InStr(31, 'Found on the Net', 'f',0) FROM t234", 0);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testInstrrev(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT InstrRev('alphabet', 'a') FROM t234", 5);
        checkQuery("SELECT InstrRev('alphabet', 'a',-1) FROM t234", 5);
        checkQuery("SELECT InstrRev('alphabet', 'a',1) FROM t234", 1);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testIsDate(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT IsDate(#1/2/2003 10:42:58 PM#) FROM t234", true);
        checkQuery("SELECT IsDate(#11/22/2003 10:42:58 PM#) FROM t234", true);
        checkQuery("SELECT IsDate('11/22/2003 10:42:58 PM') FROM t234", true);
        checkQuery("SELECT IsDate('january 3,2015') FROM t234", true);
        // fails in JDK8: checkQuery("SELECT isDate('janr 3,2015') FROM t234", false);
        checkQuery("SELECT IsDate('03 3,2015') FROM t234", true);
        checkQuery("SELECT IsDate('3 3,2015') FROM t234", true);
        // fails in JDK8: checkQuery("SELECT isDate('Fri Feb 10 00:25:09 CET 2012') FROM t234", false);
        checkQuery("SELECT IsDate('Fri Feb 10 2012') FROM t234", false);
        checkQuery("SELECT IsDate('Fri Feb 10 00:25:09 2012') FROM t234", false);
        // fails in JDK8: checkQuery("SELECT isDate('Fri Feb 10 00:25:09') FROM t234", false);
        // fails in JDK8: checkQuery("SELECT isDate('jan 35,2015') FROM t234", false);
        checkQuery("SELECT IsDate('Feb 20 01:25:09 PM') FROM t234", true);
        // fails in JDK8: checkQuery("SELECT isDate('Feb 10 00:25:09') FROM t234", true);
        // fails in JDK8: checkQuery("SELECT isDate('02 10 00:25:09') FROM t234", true);
        // fails in JDK8: checkQuery("SELECT isDate('Feb 35 00:25:09') FROM t234", true);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testSimpleDateFormatLenientTrue(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        // format taken from:
        // checkQuery("SELECT isDate('Feb 10 00:25:09') FROM t234", true);

        SimpleDateFormat sdf = new SimpleDateFormat("MMM dd hh:mm:ss");
        sdf.setLenient(true); // fails with lenient = false, see next test case
        Date parsedDate = sdf.parse("Feb 10 00:25:09");
        assertNotNull(parsedDate);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testIsNumber(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT IsNumeric(33) FROM t234", true);
        checkQuery("SELECT IsNumeric('33') FROM t234", true);
        checkQuery("SELECT IsNumeric('a') FROM t234", false);
        checkQuery("SELECT IsNumeric('33d') FROM t234", false);
        checkQuery("SELECT IsNumeric(id) FROM t234", true);
        checkQuery("SELECT IsNumeric('4,5') FROM t234", true);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testLcase(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT LCASE(' SAAxxxx   ') FROM t234", " saaxxxx   ");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testLeft(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Left('Found on the Net', 4), Left(null, 4) FROM t234", "Foun", null);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testLen(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT len('1222sssss.3hhh'), len(null) FROM t234", 14, null);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testLTrim(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT LTRIM(' SSS   ') FROM t234", "SSS   ");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testMid(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery(
            "SELECT Mid ('Found on the Net', 2, 4), Mid ('Found on the Net', 1, 555),Mid(null, 1, 555) FROM t234",
            "ound", "Found on the Net", null);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testMinute(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Minute(#10:42:58 pM#),Minute(#10:42:58 AM#),Minute(#11/22/2003 10:42:58 PM#) FROM t234", 42, 42, 42);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testMonth(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT month(#11/22/2003 10:42:58 PM#) FROM t234", 11);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testNow(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        // ensure enough time left in the current second to avoid test failure
        while (System.currentTimeMillis() % 1000 > 900) {
            Thread.sleep(25L);
        }
        Date now = new Date(System.currentTimeMillis() / 1000 * 1000);
        checkQuery("SELECT now() FROM t234", now);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testTime(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        Calendar cl = Calendar.getInstance();
        cl.set(Calendar.MILLISECOND, 0);
        cl.set(1899, 11, 30);
        checkQuery("SELECT time() FROM t234", cl.getTime());
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testReplace(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Replace('alphabet', 'bet', 'hydr') FROM t234", "alphahydr");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testRight(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Right ('Tech on the Net', 3),Right(null,12) FROM t234", "Net", null);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testRtrim(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT RTRIM(' SSS   ') FROM t234", " SSS");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testSecond(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Second(#10:42:58 pM#),Second(#10:42:58 AM#),Second(#11/22/2003 10:42:58 PM#) FROM t234", 58,
            58, 58);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testSin(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT sin(1) FROM t234", 0.8414709848078965);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testSpace(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT space(5) FROM t234", "     ");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testTrim(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT TRIM(' SSS   ') FROM t234", "SSS");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testUcase(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT UCASE(' SAAxxxx   ') FROM t234", " SAAXXXX   ");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testVal(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery(
            "SELECT val('0.'), Val('hhh'), val('.a'), val('.'), val('.44'), Val('1222.3hhh'), Val('12 22.3hhh'), VAL('-'), VAL('-2,3') FROM t234",
            0.0, 0.0, 0.0, 0.0, 0.44, 1222.3, 1222.3, 0.0, -2.0);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testYear(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT YEAR(#11/22/2003 10:42:58 PM#) FROM t234", 2003);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testDateDiff(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT DateDiff('yyyy',#11/22/1992 10:42:58 PM#,#11/22/2007 10:42:58 AM#) FROM t234", 15);
        checkQuery("SELECT DateDiff('y',#11/22/1992 11:00:00 AM#,#11/22/2007 10:00:00 AM#) FROM t234", 5478);
        checkQuery("SELECT DateDiff('y',#11/22/1992 11:00:00 AM#,#11/22/2007 12:00:00 AM#) FROM t234", 5478);
        checkQuery("SELECT DateDiff('yyyy',#11/22/1992 10:42:58 PM#,#11/22/2007 10:42:58 AM#) FROM t234", 15);
        checkQuery("SELECT DateDiff('m',#11/22/2007 11:00:00 AM#,#11/22/2007 10:00:00 AM#) FROM t234", 0);
        checkQuery("SELECT DateDiff('m',#11/22/1992 11:00:00 AM#,#08/22/2007 12:00:00 AM#) FROM t234", 177);
        checkQuery("SELECT DateDiff('d',#1/1/2004 11:00:00 AM#,#1/3/2004 11:00:00 AM#) FROM t234", 2);
        checkQuery("SELECT DateDiff('d',#11/22/1992 11:00:00 AM#,#11/22/2007 10:00:00 AM#) FROM t234", 5478);
        checkQuery("SELECT DateDiff('d',#11/22/1992 11:00:00 AM#,#11/22/2007 12:00:00 AM#) FROM t234", 5478);
        checkQuery("SELECT DateDiff('w',#11/22/1992 11:00:00 AM#,#11/22/2007 10:00:00 AM#) FROM t234", 782);
        checkQuery("SELECT DateDiff('w',#11/22/1992 11:00:00 AM#,#11/22/2007 12:00:00 AM#) FROM t234", 782);
        checkQuery("SELECT DateDiff('ww',#11/22/1992 11:00:00 AM#,#11/22/2007 10:00:00 AM#) FROM t234", 782);
        checkQuery("SELECT DateDiff('ww',#11/22/1992 11:00:00 AM#,#11/22/2007 12:00:00 AM#) FROM t234", 782);
        checkQuery("SELECT DateDiff('ww',#10/22/2007 11:00:00 AM#,#11/22/2007 10:00:00 AM#) FROM t234", 4);
        checkQuery("SELECT DateDiff('ww',#07/22/2007 11:00:00 AM#,#11/22/2007 12:00:00 AM#) FROM t234", 17);
        checkQuery("SELECT DateDiff('h',#10/22/2007 08:01:00 AM#,#10/22/2006 04:00:00 AM#) FROM t234", -8764);
        checkQuery("SELECT DateDiff('h',#10/22/2007 10:07:00 AM#,#10/22/2007 11:07:00 AM#) FROM t234", 1);
        checkQuery("SELECT DateDiff('h',#10/22/2007 11:00:00 AM#,#10/22/2007 10:07:00 AM#) FROM t234", -1);

        checkQuery("SELECT DateDiff('n',#10/22/2007 08:00:00 AM#,#10/22/2003 04:00:00 AM#) FROM t234", -2104080);
        checkQuery("SELECT DateDiff('h',#10/22/2007 08:00:00 AM#,#10/22/2005 04:00:00 AM#) FROM t234", -17524);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testDatePart(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT DatePart('yyyy',#11/22/1992 10:42:58 PM#) FROM t234", 1992);
        checkQuery("SELECT DatePart('q',#11/22/1992 10:42:58 PM#) FROM t234", 4);
        checkQuery("SELECT DatePart('d',#11/22/1992 10:42:58 PM#) FROM t234", 22);
        checkQuery("SELECT DatePart('y',#11/22/1992 10:42:58 PM#) FROM t234", 327);
        checkQuery("SELECT DatePart('ww',#11/22/1992 10:42:58 PM#) FROM t234", 48);
        checkQuery("SELECT DatePart('ww',#11/22/2006 10:42:58 PM#,3) FROM t234", 48);
        checkQuery("SELECT DatePart('w',#05/8/2013#,7), datePart('ww',#11/22/2006 10:42:58 PM#,6,3) FROM t234", 5, 46);
        checkQuery("SELECT DatePart('w',#05/13/1992 10:42:58 PM#) FROM t234", 4);
        checkQuery("SELECT DatePart('h',#05/13/1992 10:42:58 PM#) FROM t234", 22);
        checkQuery("SELECT DatePart('n',#05/13/1992 10:42:58 PM#) FROM t234", 42);
        checkQuery("SELECT DatePart('s',#05/13/1992 10:42:58 PM#) FROM t234", 58);

    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testDateSerial(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

        checkQuery("SELECT DateSerial(1998,5, 10) FROM t234", sdf.parse("1998-05-10 00:00:00"));
        checkQuery("SELECT 'It works, I can''t believe it.' FROM t234 WHERE #05/13/1992#=dateserial(1992,05,13)",
            "It works, I can't believe it.");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testFormatNumber(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Format(number,'percent') FROM tblFormat", "");
        checkQuery("SELECT Format(0.981,'percent') FROM t234", "98.10%");
        checkQuery("SELECT Format(num,'fixed') FROM t234", "-1110.55");
        checkQuery("SELECT Format(num,'standard') FROM t234", "-1,110.55");
        checkQuery("SELECT Format(num,'general number') FROM t234", "-1110.554");
        checkQuery("SELECT Format(num,'on/off') FROM t234", "On");
        checkQuery("SELECT Format(num,'true/false') FROM t234", "True");
        checkQuery("SELECT Format(num,'yes/no') FROM t234", "Yes");
        checkQuery("SELECT Format (11111210.6, '#,##0.00') FROM t234", "11,111,210.60");
        checkQuery("SELECT Format (1111111210.6, 'Scientific') FROM t234", "1.11E+09");
        checkQuery("SELECT Format (0.00000000000000015661112106, 'Scientific') FROM t234", "1.57E-16");
        Locale prevLocale = Locale.getDefault();
        Locale.setDefault(Locale.US);
        checkQuery("SELECT Format(1.239, 'Currency') FROM t234", "$1.24");
        Locale.setDefault(prevLocale);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testTimestamp(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT #2006-12-11#=timestamp '2006-12-11 00:00:00' FROM dual", true);
        checkQuery("SELECT #2006-12-11 1:2:3#=timestamp '2006-12-11 01:02:03' FROM dual", true);
        checkQuery("SELECT #2006-2-1 1:2:3#=timestamp '2006-02-01 01:02:03' FROM dual", true);
        checkQuery("SELECT #2/1/2006 1:2:3#=timestamp '2006-02-01 01:02:03' FROM dual", true);
        checkQuery("SELECT #12/11/2006 1:2:3#=timestamp '2006-12-11 01:02:03' FROM dual", true);
        checkQuery("SELECT #1392-01-10 1:2:3#=timestamp '1392-01-02 01:02:03' FROM dual", true);

        checkQuery("SELECT #12/11/2006 1:2:3 am#=timestamp '2006-12-11 01:02:03' FROM dual", true);
        checkQuery("SELECT #12/11/2006 1:2:3 pm#=timestamp '2006-12-11 13:02:03' FROM dual", true);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testFormatDate(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Format(date, 'Short date') FROM tblFormat", "");
        checkQuery("SELECT Format(#05/13/1994 10:42:58 PM#, 'Long date') FROM t234", "Friday, May 13, 1994");

        checkQuery("SELECT Format(#05/13/1994 10:42:58 PM#, 'Short date') FROM t234", "5/13/1994");
        checkQuery("SELECT Format(#05/13/1994 10:42:58 AM#, 'Long time') FROM t234", "10:42:58 AM");

        checkQuery("SELECT Format(#05/13/1994 10:42:58 PM#, 'Short time') FROM t234", "22:42");
        checkQuery("SELECT Format(#05/13/1994 10:42:58 PM#, 'General date') FROM t234", "5/13/1994 10:42:58 PM");

        checkQuery("SELECT Format(#05/13/1994 10:42:58 PM#, 'Medium date') FROM t234", "13-May-94");

        checkQuery("SELECT Format(#05/13/1994 10:42:18 PM#, 'Medium time') FROM t234", "10:42 PM");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testSign(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT sign(0), sign(-20.4), sign(4) FROM t234", 0, -1, 1);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testWeekDayName(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT WeekDayName(3) FROM t234", "Tuesday");
        checkQuery("SELECT WeekDayName(3,true) FROM t234", "Tue");
        checkQuery("SELECT WeekdayName (3, TRUE, 2) FROM t234", "Wed");
        dumpQueryResult("SELECT WeekdayName(Weekday(#2001-1-1#)) FROM t234");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testMonthName(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT MonthName(3) FROM t234", "March");
        checkQuery("SELECT MonthName(3, true) FROM t234", "Mar");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testStr(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT str(id), str(num), str(4.5555555) FROM t234", " 1234", "-1110.554", " 4.5555555");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testDateValue(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

        checkQuery("SELECT dateValue(#11/22/2003 10:42:58 PM#) FROM t234",
            sdf.parse("2003-11-22 00:00:00.0"));
        checkQuery("SELECT dateValue(#11/22/2003 21:42:58 AM#) FROM t234",
            sdf.parse("2003-11-22 00:00:00.0"));
        checkQuery("SELECT dateValue('6/30/2004') FROM t234", sdf.parse("2004-06-30 00:00:00.0"));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testFormatString(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Format(text,'Long date') FROM tblFormat", "");
        checkQuery("SELECT Format('05/13/1994','Long date') FROM t234", "Friday, May 13, 1994");
        checkQuery("SELECT Format(0.6,'percent') FROM t234", "60.00%");
        checkQuery("SELECT Format('0,6','percent') FROM t234", "600.00%");
        // beware of bug http://bugs.java.com/view_bug.do?bug_id=7131459 !
        checkQuery("SELECT Format(48.14251, '.###') FROM t234", "48.143");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testInt(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT int(1111112.5), int(-2.5) FROM t234", 1111112, -3);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testRnd(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        dumpQueryResult("SELECT rnd() FROM t234");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testStrComp(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT StrComp('Cia','Cia') FROM t234", 0);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testStrConv(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT StrConv('Cia',1) FROM t234", "CIA");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testStrReverse(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT StrReverse('ylatI') FROM t234", "Italy");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testString(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT String(4,'c') FROM t234", "cccc");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testWeekday(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Weekday(#06/27/2013 10:42:58 PM#,1) FROM t234", 5);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testFinancial(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery(
            "SELECT FV(0,100,-100,-10000,-1), DDB(1001100,10020,111,62,5.5), NPer(0.0525,200,1500,233,0.1), IPmt(0.5,4,8,10*1,10000,0.5), PV(0,4,-10000,1000,-1.55),PPmt(0.5,3,7,100000,15000.1),SLN(10000,110000,9),SYD(10000,200,12,4),Pmt(0.08,30,5000,-15000,0.1) FROM t234",
            20000.0, 2234.68083152805, -7.721791247488574, 477.63917525773195, 39000.0, -8042.7461874696455,
            -11111.111111111111, 1130.7692307692307, -311.72566612727735);
        checkQuery("SELECT Rate(3,200,-610,0,-20,0.1) FROM t234", -0.01630483472667564);
    }
}
