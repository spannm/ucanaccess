package net.ucanaccess.jdbc;

import net.ucanaccess.converters.Functions;
import net.ucanaccess.test.AccessVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Locale;
import java.util.Objects;

class FunctionsTest extends UcanaccessBaseTest {

    @BeforeAll
    static void setLocale() {
        locale = Locale.getDefault();
        Locale.setDefault(Locale.US);
    }

    @AfterAll
    static void resetLocale() {
        Locale.setDefault(Objects.requireNonNullElseGet(locale, Locale::getDefault));
    }

    @Override
    protected String getAccessPath() {
        return getTestDbDir() + "functions" + getFileFormat().name() + getFileFormat().getFileExtension();
    }

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements(
            "CREATE TABLE t_format (id int NOT NULL PRIMARY KEY, text TEXT, date DATETIME, number NUMERIC)",
            "INSERT INTO t_format (id) VALUES(1)");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testASC(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Asc('A') FROM t_funcs", singleRec(65));
        checkQuery("SELECT Asc('1') FROM t_funcs", singleRec(49));
        checkQuery("SELECT Asc('u') FROM t_funcs", singleRec(117));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testSwitch(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Switch('1'='1', 1, false, 2, true, 1) FROM t_funcs");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testATN(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Atn(3) FROM t_funcs", singleRec(1.2490457723982544));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testNz(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Nz(null,'lampredotto'), Nz('turtelaz','lampredotto'), Nz(null, 1.5), Nz(2, 2) FROM t_funcs",
            singleRec("lampredotto", "turtelaz", 1.5, 2));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testCBoolean(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT CBool(id), CBool(1=2), CBool('true'), CBool('false'), CBool(0), CBool(-3) FROM t_funcs",
            singleRec(true, false, true, false, false, true));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testCVar(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT CVAR(8), CVAR(8.44) FROM t_funcs", singleRec("8", "8.44"));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testCstr(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT CStr(date0) FROM t_funcs", singleRec("11/22/2003 10:42:58 PM"));
        checkQuery("SELECT CStr(false) FROM t_funcs", singleRec("false"));
        checkQuery("SELECT CStr(8) FROM t_funcs", singleRec("8"));
        checkQuery("SELECT CStr(8.78787878) FROM t_funcs", singleRec("8.78787878"));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testCsign(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT CSign(8.53453543) FROM t_funcs", singleRec(8.534535));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testCDate(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        checkQuery("SELECT CDate('Apr 6, 2003') FROM t_funcs", recs(rec(sdf.parse("2003-04-06 00:00:00.0"))));
        checkQuery("SELECT CDate('1582-10-15') FROM t_funcs", recs(rec(sdf.parse("1582-10-15 00:00:00.0"))));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testCLong(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT CLong(8.52), CLong(8.49), CLong(5.5) FROM t_funcs", singleRec(9, 8, 6));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testCLng(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT CLng(8.52), CLng(8.49), CLng(5.5) FROM t_funcs", singleRec(9, 8, 6));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testCDec(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT CDec(8.45 * 0.005 * 0.01) FROM t_funcs", singleRec(0.0004225));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testCcur(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT CCur(123.4567812), CCur(123.4547812) FROM t_funcs", singleRec(123.4568, 123.4548));
        checkQuery("SELECT CCur(0.552222211) * 100 FROM t_funcs", singleRec(55.22));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testCint(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT CInt(8.51), CInt(4.5) FROM t_funcs", singleRec(9, 4));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testChr(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Chr(65) FROM t_funcs", singleRec("A"));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testCos(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Cos(1) FROM t_funcs", singleRec(0.5403023058681398));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testCurrentUser(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT CurrentUser() FROM t_funcs", singleRec("ucanaccess"));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testDateAdd(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

        checkQuery("SELECT DateAdd('YYYY', 4, #11/22/2003 10:42:58 PM#) FROM t_funcs", recs(rec(sdf.parse("2007-11-22 22:42:58"))));
        checkQuery("SELECT DateAdd('Q', 3 ,#11/22/2003 10:42:58 PM#) FROM t_funcs", recs(rec(sdf.parse("2004-08-22 22:42:58"))));
        checkQuery("SELECT DateAdd('Y', 451 ,#11/22/2003 10:42:58 PM#) FROM t_funcs", recs(rec(sdf.parse("2005-02-15 22:42:58"))));
        checkQuery("SELECT DateAdd('D', 451 ,#11/22/2003 10:42:58 PM#) FROM t_funcs", recs(rec(sdf.parse("2005-02-15 22:42:58"))));
        checkQuery("SELECT DateAdd('Y', 45 ,#11/22/2003 10:42:58 PM#) FROM t_funcs", recs(rec(sdf.parse("2004-01-06 22:42:58"))));
        checkQuery("SELECT DateAdd('D', 45 ,#11/22/2003 10:42:58 PM#) FROM t_funcs", recs(rec(sdf.parse("2004-01-06 22:42:58"))));
        checkQuery("SELECT DateAdd('Y', 4 ,#11/22/2003 10:42:58 PM#) FROM t_funcs", recs(rec(sdf.parse("2003-11-26 22:42:58"))));
        checkQuery("SELECT DateAdd('D', 4 ,#11/22/2003 10:42:58 PM#) FROM t_funcs", recs(rec(sdf.parse("2003-11-26 22:42:58"))));
        checkQuery("SELECT DateAdd('W', 43 ,#11/22/2003 10:42:58 PM#) FROM t_funcs", recs(rec(sdf.parse("2004-01-04 22:42:58"))));
        checkQuery("SELECT DateAdd('W', 1 ,#11/22/2003 10:42:58 PM#) FROM t_funcs", recs(rec(sdf.parse("2003-11-23 22:42:58"))));
        checkQuery("SELECT DateAdd('WW', 43 ,#11/22/2003 10:42:58 PM#) FROM t_funcs", recs(rec(sdf.parse("2004-09-18 22:42:58"))));
        checkQuery("SELECT DateAdd('H', 400 ,#11/22/2003 10:42:58 PM#) FROM t_funcs", recs(rec(sdf.parse("2003-12-09 14:42:58"))));
        checkQuery("SELECT DateAdd('M', 400 ,#11/22/2003 10:42:58 PM#) FROM t_funcs", recs(rec(sdf.parse("2037-03-22 22:42:58"))));
        checkQuery("SELECT DateAdd('S', 400 ,#11/22/2003 10:42:58 PM#) FROM t_funcs", recs(rec(sdf.parse("2003-11-22 22:49:38"))));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testDate(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Date() FROM t_funcs");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testDay(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Day(#11/22/2003 10:42:58 PM#) FROM t_funcs", singleRec(22));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testExp(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Exp(3.1), exp(0.4) FROM t_funcs", singleRec(22.197951281441636, 1.4918246976412703d));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testHour(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Hour(#10:42:58 pM#), Hour(#10:42:58 AM#), Hour(#11/22/2003 10:42:58 PM#) FROM t_funcs", singleRec(22, 10, 22));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testIIf(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT IIf(IsNull(descr)=true, 'pippo', 'pl''uto'&'\" \" cccc'), IIf(IsNull(descr)=true, 'pippo', 'pl''uto'&'\" \" cccc') "
            + "FROM t_funcs", singleRec("pl'uto\" \" cccc", "pl'uto\" \" cccc"));
        checkQuery("SELECT IIf(true, false, true) FROM t_funcs", singleRec(false));
        checkQuery("SELECT IIf('pippo'=null, 'capra', 'd''una capra') FROM t_funcs", singleRec("d'una capra"));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testInstr(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT InStr('Found on the Net', 'the') FROM t_funcs", singleRec(10));
        checkQuery("SELECT InStr('Found on the Net', 'f') FROM t_funcs", singleRec(1));
        checkQuery("SELECT InStr(1, 'Found on the Net', 'f') FROM t_funcs", singleRec(1));
        checkQuery("SELECT InStr(1, 'Found on the Net', 'f',1) FROM t_funcs", singleRec(1));
        checkQuery("SELECT InStr(1, 'Found on the Net', 't',0) FROM t_funcs", singleRec(10));
        checkQuery("SELECT InStr(31, 'Found on the Net', 'f',0) FROM t_funcs", singleRec(0));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testInstrrev(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT InstrRev('alphabet', 'a') FROM t_funcs", singleRec(5));
        checkQuery("SELECT InstrRev('alphabet', 'a',-1) FROM t_funcs", singleRec(5));
        checkQuery("SELECT InstrRev('alphabet', 'a',1) FROM t_funcs", singleRec(1));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testIsDate(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT IsDate(#1/2/2003 10:42:58 PM#) FROM t_funcs", singleRec(true));

        checkQuery("SELECT IsDate(#11/22/2003 10:42:58 PM#) FROM t_funcs", singleRec(true));

        checkQuery("SELECT IsDate('11/22/2003 10:42:58 PM') FROM t_funcs", singleRec(true));

        checkQuery("SELECT IsDate('january 3,2015') FROM t_funcs", singleRec(true));

        // fails in JDK8: checkQuery("SELECT isDate('janr 3,2015') FROM " + TBL, false);
        checkQuery("SELECT IsDate('03 3,2015') FROM t_funcs", singleRec(true));
        checkQuery("SELECT IsDate('3 3,2015') FROM t_funcs", singleRec(true));
        // fails in JDK8: checkQuery("SELECT isDate('Fri Feb 10 00:25:09 CET 2012') FROM " + TBL, false);
        checkQuery("SELECT IsDate('Fri Feb 10 2012') FROM t_funcs", singleRec(false));
        checkQuery("SELECT IsDate('Fri Feb 10 00:25:09 2012') FROM t_funcs", singleRec(false));
        // fails in JDK8: checkQuery("SELECT isDate('Fri Feb 10 00:25:09') FROM " + TBL, false);
        // fails in JDK8: checkQuery("SELECT isDate('jan 35,2015') FROM " + TBL, false);
        checkQuery("SELECT IsDate('Feb 20 01:25:09 PM') FROM t_funcs", singleRec(true));
        // fails in JDK8: checkQuery("SELECT isDate('Feb 10 00:25:09') FROM " + TBL, true);
        // fails in JDK8: checkQuery("SELECT isDate('02 10 00:25:09') FROM " + TBL, true);
        // fails in JDK8: checkQuery("SELECT isDate('Feb 35 00:25:09') FROM " + TBL, true);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testSimpleDateFormatLenientTrue(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        // format taken from:
        // checkQuery("SELECT isDate('Feb 10 00:25:09') FROM " + TBL, true);

        SimpleDateFormat sdf = new SimpleDateFormat("MMM dd hh:mm:ss");
        sdf.setLenient(true); // fails with lenient = false, see next test case
        Date parsedDate = sdf.parse("Feb 10 00:25:09");
        assertNotNull(parsedDate);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testIsNumber(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT IsNumeric(33) FROM t_funcs", singleRec(true));
        checkQuery("SELECT IsNumeric('33') FROM t_funcs", singleRec(true));
        checkQuery("SELECT IsNumeric('a') FROM t_funcs", singleRec(false));
        checkQuery("SELECT IsNumeric('33d') FROM t_funcs", singleRec(false));
        checkQuery("SELECT IsNumeric(id) FROM t_funcs", singleRec(true));
        checkQuery("SELECT IsNumeric('4,5') FROM t_funcs", singleRec(true));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testLcase(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT LCASE(' SAAxxxx   ') FROM t_funcs", singleRec(" saaxxxx   "));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testLeft(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Left('Found on the Net', 4), Left(null, 4) FROM t_funcs", singleRec("Foun", null));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testLen(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT len('1222sssss.3hhh'), len(null) FROM t_funcs", singleRec(14, null));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testLTrim(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT LTRIM(' SSS   ') FROM t_funcs", singleRec("SSS   "));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testMid(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Mid ('Found on the Net', 2, 4), Mid ('Found on the Net', 1, 555),Mid(null, 1, 555) FROM t_funcs", singleRec("ound", "Found on the Net", null));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testMinute(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Minute(#10:42:58 pM#),Minute(#10:42:58 AM#),Minute(#11/22/2003 10:42:58 PM#) FROM t_funcs", singleRec(42, 42, 42));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testMonth(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT month(#11/22/2003 10:42:58 PM#) FROM t_funcs", singleRec(11));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testNow(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        sleepUntilStartOfNewSecond();
        Date now = Date.from(LocalDateTime.now()
            .truncatedTo(ChronoUnit.SECONDS)
            .atZone(ZoneId.systemDefault()).toInstant());
        checkQuery("SELECT Now() FROM t_funcs", singleRec(now));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testTime(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        sleepUntilStartOfNewSecond();
        Date time = Date.from(LocalDateTime.now()
            .withYear(1899).withMonth(12).withDayOfMonth(30)
            .truncatedTo(ChronoUnit.SECONDS)
            .atZone(ZoneId.systemDefault()).toInstant());
        checkQuery("SELECT Time() FROM t_funcs", recs(rec(time)));
    }

    /**
     * Ensure enough time left in the current second to avoid failure of time-sensitive tests
     */
    private static void sleepUntilStartOfNewSecond() {
        while (System.currentTimeMillis() % 1000 > 900) {
            try {
                Thread.sleep(25L);
            } catch (InterruptedException _ex) {
                return;
            }
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testReplace(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Replace('alphabet', 'bet', 'hydr') FROM t_funcs", singleRec("alphahydr"));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testRight(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Right('Tech on the Net', 3), Right(null,12) FROM t_funcs", singleRec("Net", null));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testRtrim(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT RTRIM(' SSS   ') FROM t_funcs", singleRec(" SSS"));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testSecond(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Second(#10:42:58 pM#), Second(#10:42:58 AM#), Second(#11/22/2003 10:42:58 PM#) FROM t_funcs", singleRec(58, 58, 58));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testSin(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT sin(1) FROM t_funcs", singleRec(0.8414709848078965));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testSpace(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT space(5) FROM t_funcs", singleRec("     "));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testTrim(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT TRIM(' SSS   ') FROM t_funcs", singleRec("SSS"));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testUcase(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT UCASE(' SAAxxxx   ') FROM t_funcs", singleRec(" SAAXXXX   "));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testVal(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT val('0.'), Val('hhh'), val('.a'), val('.'), val('.44'), Val('1222.3hhh'), Val('12 22.3hhh'), VAL('-'), VAL('-2,3') "
            + "FROM t_funcs", singleRec(0.0, 0.0, 0.0, 0.0, 0.44, 1222.3, 1222.3, 0.0, -2.0));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testYear(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT YEAR(#11/22/2003 10:42:58 PM#) FROM t_funcs", singleRec(2003));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testDateDiff(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT DateDiff('yyyy',#11/22/1992 10:42:58 PM#,#11/22/2007 10:42:58 AM#) FROM t_funcs", singleRec(15));
        checkQuery("SELECT DateDiff('y',#11/22/1992 11:00:00 AM#,#11/22/2007 10:00:00 AM#) FROM t_funcs", singleRec(5478));
        checkQuery("SELECT DateDiff('y',#11/22/1992 11:00:00 AM#,#11/22/2007 12:00:00 AM#) FROM t_funcs", singleRec(5478));
        checkQuery("SELECT DateDiff('yyyy',#11/22/1992 10:42:58 PM#,#11/22/2007 10:42:58 AM#) FROM t_funcs", singleRec(15));
        checkQuery("SELECT DateDiff('m',#11/22/2007 11:00:00 AM#,#11/22/2007 10:00:00 AM#) FROM t_funcs", singleRec(0));
        checkQuery("SELECT DateDiff('m',#11/22/1992 11:00:00 AM#,#08/22/2007 12:00:00 AM#) FROM t_funcs", singleRec(177));
        checkQuery("SELECT DateDiff('d',#1/1/2004 11:00:00 AM#,#1/3/2004 11:00:00 AM#) FROM t_funcs", singleRec(2));
        checkQuery("SELECT DateDiff('d',#11/22/1992 11:00:00 AM#,#11/22/2007 10:00:00 AM#) FROM t_funcs", singleRec(5478));
        checkQuery("SELECT DateDiff('d',#11/22/1992 11:00:00 AM#,#11/22/2007 12:00:00 AM#) FROM t_funcs", singleRec(5478));
        checkQuery("SELECT DateDiff('w',#11/22/1992 11:00:00 AM#,#11/22/2007 10:00:00 AM#) FROM t_funcs", singleRec(782));
        checkQuery("SELECT DateDiff('w',#11/22/1992 11:00:00 AM#,#11/22/2007 12:00:00 AM#) FROM t_funcs", singleRec(782));
        checkQuery("SELECT DateDiff('ww',#11/22/1992 11:00:00 AM#,#11/22/2007 10:00:00 AM#) FROM t_funcs", singleRec(782));
        checkQuery("SELECT DateDiff('ww',#11/22/1992 11:00:00 AM#,#11/22/2007 12:00:00 AM#) FROM t_funcs", singleRec(782));
        checkQuery("SELECT DateDiff('ww',#10/22/2007 11:00:00 AM#,#11/22/2007 10:00:00 AM#) FROM t_funcs", singleRec(4));
        checkQuery("SELECT DateDiff('ww',#07/22/2007 11:00:00 AM#,#11/22/2007 12:00:00 AM#) FROM t_funcs", singleRec(17));
        checkQuery("SELECT DateDiff('h',#10/22/2007 08:01:00 AM#,#10/22/2006 04:00:00 AM#) FROM t_funcs", singleRec(-8764));
        checkQuery("SELECT DateDiff('h',#10/22/2007 10:07:00 AM#,#10/22/2007 11:07:00 AM#) FROM t_funcs", singleRec(1));
        checkQuery("SELECT DateDiff('h',#10/22/2007 11:00:00 AM#,#10/22/2007 10:07:00 AM#) FROM t_funcs", singleRec(-1));
        checkQuery("SELECT DateDiff('n',#10/22/2007 08:00:00 AM#,#10/22/2003 04:00:00 AM#) FROM t_funcs", singleRec(-2104080));
        checkQuery("SELECT DateDiff('h',#10/22/2007 08:00:00 AM#,#10/22/2005 04:00:00 AM#) FROM t_funcs", singleRec(-17524));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testDatePart(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT DatePart('yyyy',#11/22/1992 10:42:58 PM#) FROM t_funcs", singleRec(1992));
        checkQuery("SELECT DatePart('q',#11/22/1992 10:42:58 PM#) FROM t_funcs", singleRec(4));
        checkQuery("SELECT DatePart('d',#11/22/1992 10:42:58 PM#) FROM t_funcs", singleRec(22));
        checkQuery("SELECT DatePart('y',#11/22/1992 10:42:58 PM#) FROM t_funcs", singleRec(327));
        checkQuery("SELECT DatePart('ww',#11/22/1992 10:42:58 PM#) FROM t_funcs", singleRec(48));
        checkQuery("SELECT DatePart('ww',#11/22/2006 10:42:58 PM#,3) FROM t_funcs", singleRec(48));
        checkQuery("SELECT DatePart('w',#05/8/2013#,7), datePart('ww',#11/22/2006 10:42:58 PM#,6,3) FROM t_funcs", singleRec(5, 46));
        checkQuery("SELECT DatePart('w',#05/13/1992 10:42:58 PM#) FROM t_funcs", singleRec(4));
        checkQuery("SELECT DatePart('h',#05/13/1992 10:42:58 PM#) FROM t_funcs", singleRec(22));
        checkQuery("SELECT DatePart('n',#05/13/1992 10:42:58 PM#) FROM t_funcs", singleRec(42));
        checkQuery("SELECT DatePart('s',#05/13/1992 10:42:58 PM#) FROM t_funcs", singleRec(58));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testDateSerial(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        checkQuery("SELECT DateSerial(1998,5, 10) FROM t_funcs", recs(rec(sdf.parse("1998-05-10 00:00:00"))));
        checkQuery("SELECT 'It works, I can''t believe it.' FROM t_funcs" + " WHERE #05/13/1992#=dateserial(1992,05,13)",
            singleRec("It works, I can't believe it."));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testFormatNumber(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Format(number, 'percent') FROM t_format", singleRec(""));
        checkQuery("SELECT Format(0.981, 'percent') FROM t_funcs", singleRec("98.10%"));
        checkQuery("SELECT Format(num, 'fixed') FROM t_funcs", singleRec("-1110.55"));
        checkQuery("SELECT Format(num, 'standard') FROM t_funcs", singleRec("-1,110.55"));
        checkQuery("SELECT Format(num, 'general number') FROM t_funcs", singleRec("-1110.554"));
        checkQuery("SELECT Format(num, 'on/off') FROM t_funcs", singleRec("On"));
        checkQuery("SELECT Format(num, 'true/false') FROM t_funcs", singleRec("True"));
        checkQuery("SELECT Format(num, 'yes/no') FROM t_funcs", singleRec("Yes"));
        checkQuery("SELECT Format(11111210.6, '#,##0.00') FROM t_funcs", singleRec("11,111,210.60"));
        checkQuery("SELECT Format(1111111210.6, 'Scientific') FROM t_funcs", singleRec("1.11E+09"));
        checkQuery("SELECT Format(0.00000000000000015661112106, 'Scientific') FROM t_funcs", singleRec("1.57E-16"));
        checkQuery("SELECT Format(1.239, 'Currency') FROM t_funcs", singleRec("$1.24"));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testTimestamp(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT #2006-12-11#=timestamp '2006-12-11 00:00:00' FROM dual", singleRec(true));
        checkQuery("SELECT #2006-12-11 1:2:3#=timestamp '2006-12-11 01:02:03' FROM dual", singleRec(true));
        checkQuery("SELECT #2006-2-1 1:2:3#=timestamp '2006-02-01 01:02:03' FROM dual", singleRec(true));
        checkQuery("SELECT #2/1/2006 1:2:3#=timestamp '2006-02-01 01:02:03' FROM dual", singleRec(true));
        checkQuery("SELECT #12/11/2006 1:2:3#=timestamp '2006-12-11 01:02:03' FROM dual", singleRec(true));
        checkQuery("SELECT #1392-01-10 1:2:3#=timestamp '1392-01-02 01:02:03' FROM dual", singleRec(true));
        checkQuery("SELECT #12/11/2006 1:2:3 am#=timestamp '2006-12-11 01:02:03' FROM dual", singleRec(true));
        checkQuery("SELECT #12/11/2006 1:2:3 pm#=timestamp '2006-12-11 13:02:03' FROM dual", singleRec(true));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testFormatDate(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Format(date, 'Short date') FROM t_format", singleRec(""));
        checkQuery("SELECT Format(#05/13/1994 10:42:58 PM#, 'Long date') FROM t_funcs", singleRec("Friday, May 13, 1994"));
        checkQuery("SELECT Format(#05/13/1994 10:42:58 PM#, 'Short date') FROM t_funcs", singleRec("5/13/1994"));
        checkQuery("SELECT Format(#05/13/1994 10:42:58 AM#, 'Long time') FROM t_funcs", singleRec("10:42:58 AM"));
        checkQuery("SELECT Format(#05/13/1994 10:42:58 PM#, 'Short time') FROM t_funcs", singleRec("22:42"));
        checkQuery("SELECT Format(#05/13/1994 10:42:58 PM#, 'General date') FROM t_funcs", singleRec("5/13/1994 10:42:58 PM"));
        checkQuery("SELECT Format(#05/13/1994 10:42:58 PM#, 'Medium date') FROM t_funcs", singleRec("13-May-94"));
        checkQuery("SELECT Format(#05/13/1994 10:42:18 PM#, 'Medium time') FROM t_funcs", singleRec("10:42 PM"));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testSign(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Sign(0), Sign(-20.4), Sign(4) FROM t_funcs", singleRec(0, -1, 1));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testWeekDayName(AccessVersion _accessVersion) throws Exception {
        assertEquals("Sunday", Functions.weekdayName(1, false, 1));
        assertEquals("Monday", Functions.weekdayName(2, false, 1));
        assertEquals("Monday", Functions.weekdayName(1, false, 2));
        assertEquals("Tuesday", Functions.weekdayName(2, false, 2));
        assertEquals("Sat", Functions.weekdayName(14, true, 1));

        init(_accessVersion);
        checkQuery("SELECT WeekDayName(3) FROM t_funcs", singleRec("Tuesday"));
        checkQuery("SELECT WeekDayName(3, true) FROM t_funcs", singleRec("Tue"));
        checkQuery("SELECT WeekdayName(3, true, 2) FROM t_funcs", singleRec("Wed"));
        dumpQueryResult("SELECT WeekdayName(Weekday(#2001-1-1#)) FROM t_funcs");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testMonthName(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT MonthName(3) FROM t_funcs", singleRec("March"));
        checkQuery("SELECT MonthName(3, true) FROM t_funcs", singleRec("Mar"));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testStr(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Str(id), Str(num), Str(4.5555555) FROM t_funcs", singleRec(" 1234", "-1110.554", " 4.5555555"));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testDateValue(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        checkQuery("SELECT dateValue(#11/22/2003 10:42:58 PM#) FROM t_funcs", recs(rec(sdf.parse("2003-11-22 00:00:00.0"))));
        checkQuery("SELECT dateValue(#11/22/2003 21:42:58 AM#) FROM t_funcs", recs(rec(sdf.parse("2003-11-22 00:00:00.0"))));
        checkQuery("SELECT dateValue('6/30/2004') FROM t_funcs", recs(rec(sdf.parse("2004-06-30 00:00:00.0"))));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testFormatString(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Format(text,'Long date') FROM t_format", singleRec(""));
        checkQuery("SELECT Format('05/13/1994','Long date') FROM t_funcs", singleRec("Friday, May 13, 1994"));
        checkQuery("SELECT Format(0.6,'percent') FROM t_funcs", singleRec("60.00%"));
        checkQuery("SELECT Format('0,6','percent') FROM t_funcs", singleRec("600.00%"));
        // beware of bug http://bugs.java.com/view_bug.do?bug_id=7131459 !
        checkQuery("SELECT Format(48.14251, '.###') FROM t_funcs", singleRec("48.143"));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testInt(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT int(1111112.5), int(-2.5) FROM t_funcs", singleRec(1111112, -3));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testRnd(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        dumpQueryResult("SELECT rnd() FROM t_funcs");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testStrComp(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT StrComp('Cia','Cia') FROM t_funcs", singleRec(0));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testStrConv(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT StrConv('Cia',1) FROM t_funcs", singleRec("CIA"));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testStrReverse(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT StrReverse('ylatI') FROM t_funcs", singleRec("Italy"));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testString(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT String(4,'c') FROM t_funcs", singleRec("cccc"));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testWeekday(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT Weekday(#06/27/2013 10:42:58 PM#,1) FROM t_funcs", singleRec(5));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testFinancial(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT FV(0,100,-100,-10000,-1), DDB(1001100,10020,111,62,5.5), NPer(0.0525,200,1500,233,0.1), "
            + "IPmt(0.5,4,8,10*1,10000,0.5), PV(0,4,-10000,1000,-1.55), PPmt(0.5,3,7,100000,15000.1), SLN(10000,110000,9), "
            + "SYD(10000,200,12,4), Pmt(0.08,30,5000,-15000,0.1) FROM t_funcs",
            singleRec(20000.0, 2234.68083152805, -7.721791247488574, 477.63917525773195, 39000.0, -8042.7461874696455, -11111.111111111111, 1130.7692307692307, -311.72566612727735));
        checkQuery("SELECT Rate(3,200,-610,0,-20,0.1) FROM t_funcs", singleRec(-0.01630483472667564));
    }
}
