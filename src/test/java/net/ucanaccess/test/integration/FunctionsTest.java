/*
Copyright (c) 2012 Marco Amadei.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package net.ucanaccess.test.integration;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;

@RunWith(Parameterized.class)
public class FunctionsTest extends AccessVersionAllTest {
    private final static SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    public FunctionsTest(AccessVersion _accessVersion) {
        super(_accessVersion);
        Locale.setDefault(Locale.US);
    }

    @Override
    public String getAccessPath() {
        return "testdbs/FunctionsTest" + fileFormat.name() + fileFormat.getFileExtension();
    }

    @Test
    public void testASC() throws Exception {
        checkQuery("select ASC('A') FROM t234", 65);
        checkQuery("select ASC('1') FROM t234", 49);
        checkQuery("select ASC('u') FROM t234", 117);
    }

    @Test
    public void testSwitch() throws Exception {
        checkQuery("select switch('1'='1',1,false,2,true, 1 ) FROM t234");
    }

    @Test
    public void testATN() throws Exception {
        checkQuery("select atn(3) FROM t234", 1.2490457723982544);
    }

    @Test
    public void testNz() throws Exception {
        checkQuery("select nz(null,'lampredotto'),nz('turtelaz','lampredotto'), nz(null, 1.5), nz(2, 2) FROM t234",
                "lampredotto", "turtelaz", 1.5, 2);
    }

    @Test
    public void testCBoolean() throws Exception {
        checkQuery("select cbool(id), cbool(1=2), cbool('true'), cbool('false'), cbool(0), cbool(-3) FROM t234",
                new Object[][] { { true, false, true, false, false, true } });
    }

    @Test
    public void testCVar() throws Exception {
        checkQuery("select cvar(8),cvar(8.44) FROM t234", "8", "8.44");
    }

    @Test
    public void testCstr() throws Exception {
        checkQuery("select cstr(date0) FROM t234", "11/22/2003 10:42:58 PM");
        checkQuery("select cstr(false) FROM t234", "false");
        checkQuery("select cstr(8) FROM t234", "8");
        checkQuery("select cstr(8.78787878) FROM t234", "8.78787878");
    }

    @Test
    public void testCsign() throws Exception {
        checkQuery("select csign(8.53453543) FROM t234", 8.534535);
    }

    @Test
    public void testCDate() throws Exception {
        checkQuery("select  Cdate('Apr 6, 2003') FROM t234", SDF.parse("2003-04-06 00:00:00.0"));

        checkQuery("select  Cdate('1582-10-15') FROM t234", SDF.parse("1582-10-15 00:00:00.0"));
    }

    @Test
    public void testCLong() throws Exception {
        checkQuery("select  Clong(8.52), Clong(8.49 ),Clong(5.5) FROM t234  ", 9, 8, 6);
    }

    @Test
    public void testCLng() throws Exception {
        checkQuery("select  Clng(8.52), Clng(8.49 ),Clng(5.5) FROM t234  ", 9, 8, 6);
    }

    @Test
    public void testCDec() throws Exception {
        checkQuery("select cdec(8.45 * 0.005 * 0.01) FROM t234  ", 0.0004225);
    }

    @Test
    public void testCcur() throws Exception {
        checkQuery("select  Ccur(123.4567812),  Ccur(123.4547812) FROM t234  ", 123.4568, 123.4548);

        checkQuery("select ccur(0.552222211)*100 FROM t234  ", 55.22);
    }

    @Test
    public void testCint() throws Exception {
        checkQuery("select  Cint(8.51), Cint(4.5) FROM t234  ", 9, 4);
    }

    @Test
    public void testChr() throws Exception {
        checkQuery("select  CHR(65) FROM t234", "A");
    }

    @Test
    public void testCos() throws Exception {
        checkQuery("select cos(1) FROM t234", 0.5403023058681398);
    }

    @Test
    public void testCurrentUser() throws Exception {
        checkQuery("select CurrentUser() FROM t234", "ucanaccess");
    }

    @Test
    public void testDateAdd() throws Exception {
        checkQuery("select dateAdd('YYYY',4 ,#11/22/2003 10:42:58 PM#) FROM t234",
                SDF.parse("2007-11-22 22:42:58"));
        checkQuery("select dateAdd('Q',3 ,#11/22/2003 10:42:58 PM#) FROM t234",
                SDF.parse("2004-08-22 22:42:58"));
        checkQuery("select dateAdd('Y',451 ,#11/22/2003 10:42:58 PM#) FROM t234",
                SDF.parse("2005-02-15 22:42:58"));
        checkQuery("select dateAdd('D',451 ,#11/22/2003 10:42:58 PM#) FROM t234",
                SDF.parse("2005-02-15 22:42:58"));
        checkQuery("select dateAdd('Y',45 ,#11/22/2003 10:42:58 PM#) FROM t234",
                SDF.parse("2004-01-06 22:42:58"));
        checkQuery("select dateAdd('D',45 ,#11/22/2003 10:42:58 PM#) FROM t234",
                SDF.parse("2004-01-06 22:42:58"));
        checkQuery("select dateAdd('Y',4 ,#11/22/2003 10:42:58 PM#) FROM t234",
                SDF.parse("2003-11-26 22:42:58"));
        checkQuery("select dateAdd('D',4 ,#11/22/2003 10:42:58 PM#) FROM t234",
                SDF.parse("2003-11-26 22:42:58"));
        checkQuery("select dateAdd('W',43 ,#11/22/2003 10:42:58 PM#) FROM t234",
                SDF.parse("2004-01-04 22:42:58"));
        checkQuery("select dateAdd('W',1 ,#11/22/2003 10:42:58 PM#) FROM t234",
                SDF.parse("2003-11-23 22:42:58"));
        checkQuery("select dateAdd('WW',43 ,#11/22/2003 10:42:58 PM#) FROM t234",
                SDF.parse("2004-09-18 22:42:58"));
        checkQuery("select dateAdd('H',400 ,#11/22/2003 10:42:58 PM#) FROM t234",
                SDF.parse("2003-12-09 14:42:58"));
        checkQuery("select dateAdd('M',400 ,#11/22/2003 10:42:58 PM#) FROM t234",
                SDF.parse("2037-03-22 22:42:58"));
        checkQuery("select dateAdd('S',400 ,#11/22/2003 10:42:58 PM#) FROM t234",
                SDF.parse("2003-11-22 22:49:38"));
    }

    @Test
    public void testDate() throws Exception {
        checkQuery("select date() FROM t234");
    }

    @Test
    public void testDay() throws Exception {
        checkQuery("select day(#11/22/2003 10:42:58 PM#) FROM t234", 22);
    }

    @Test
    public void testExp() throws Exception {
        checkQuery("select exp(3.1),exp(0.4) FROM t234", 22.197951281441636, 1.4918246976412703);
    }

    @Test
    public void testHour() throws Exception {
        checkQuery("select Hour(#10:42:58 pM#),Hour(#10:42:58 AM#),Hour(#11/22/2003 10:42:58 PM#) FROM t234", 22, 10,
                22);
    }

    @Test
    public void testIif() throws Exception {
        checkQuery(
                "select  IIf(isNull(descr)=true,'pippo','pl''uto'&'\" \" cccc'),IIf(isNull(descr)=true,'pippo','pl''uto'&'\" \" cccc') FROM t234",
                "pl'uto\" \" cccc", "pl'uto\" \" cccc");

        checkQuery("select  IIf(true,false,true) FROM t234", false);
        checkQuery("select  IIf('pippo'=null,'capra','d''una capra') FROM t234", "d'una capra");
    }

    @Test
    public void testInstr() throws Exception {
        checkQuery("SELECT Instr ( 'Found on the Net', 'the') FROM t234", 10);
        checkQuery("SELECT Instr ( 'Found on the Net', 'f') FROM t234", 1);
        checkQuery("SELECT Instr ( 1,'Found on the Net', 'f') FROM t234", 1);
        checkQuery("SELECT Instr ( 1,'Found on the Net', 'f',1) FROM t234", 1);
        checkQuery("SELECT Instr ( 1,'Found on the Net', 't',0) FROM t234", 10);
        checkQuery("SELECT Instr ( 31,'Found on the Net', 'f',0) FROM t234", 0);
    }

    @Test
    public void testInstrrev() throws Exception {
        checkQuery("SELECT InstrRev ('alphabet', 'a') FROM t234", 5);
        checkQuery("SELECT InstrRev ('alphabet', 'a',-1) FROM t234", 5);
        checkQuery("SELECT InstrRev ('alphabet', 'a',1) FROM t234", 1);
    }

    @Test
    public void testIsDate() throws Exception {
        checkQuery("SELECT isDate(#1/2/2003 10:42:58 PM#) FROM t234", true);
        checkQuery("SELECT isDate(#11/22/2003 10:42:58 PM#) FROM t234", true);
        checkQuery("SELECT isDate('11/22/2003 10:42:58 PM') FROM t234", true);
        checkQuery("SELECT isDate('january 3,2015') FROM t234", true);
        // fails in JDK8: checkQuery("SELECT isDate('janr 3,2015') FROM t234", false);
        checkQuery("SELECT isDate('03 3,2015') FROM t234", true);
        checkQuery("SELECT isDate('3 3,2015') FROM t234", true);
        // fails in JDK8: checkQuery("SELECT isDate('Fri Feb 10 00:25:09 CET 2012') FROM t234", false);
        checkQuery("SELECT isDate('Fri Feb 10 2012') FROM t234", false);
        checkQuery("SELECT isDate('Fri Feb 10 00:25:09 2012') FROM t234", false);
        // fails in JDK8: checkQuery("SELECT isDate('Fri Feb 10 00:25:09') FROM t234", false);
        // fails in JDK8: checkQuery("SELECT isDate('jan 35,2015') FROM t234", false);
        checkQuery("SELECT isDate('Feb 20 01:25:09 PM') FROM t234", true);
        // fails in JDK8: checkQuery("SELECT isDate('Feb 10 00:25:09') FROM t234", true);
        // fails in JDK8: checkQuery("SELECT isDate('02 10 00:25:09') FROM t234", true);
        // fails in JDK8: checkQuery("SELECT isDate('Feb 35 00:25:09') FROM t234", true);
    }

    @Test
    public void testSimpleDateFormatLenientTrue() throws Exception {
        // format taken from:
        // checkQuery("SELECT isDate('Feb 10 00:25:09') FROM t234", true);

        SimpleDateFormat sdf = new SimpleDateFormat("MMM dd hh:mm:ss");
        sdf.setLenient(true); // fails with lenient = false, see next test case
        Date parsedDate = sdf.parse("Feb 10 00:25:09");
        assertNotNull(parsedDate);
    }

    @Test
    public void testIsNumber() throws Exception {
        checkQuery("SELECT isNumeric(33)  FROM t234", true);
        checkQuery("SELECT isNumeric('33') FROM t234", true);
        checkQuery("SELECT isNumeric('a') FROM t234", false);
        checkQuery("SELECT isNumeric('33d')from t234", false);
        checkQuery("SELECT isNumeric(id)  FROM t234", true);
        checkQuery("SELECT isNumeric('4,5')  FROM t234", true);
    }

    @Test
    public void testLcase() throws Exception {
        checkQuery("SELECT LCASE(' SAAxxxx   ') FROM t234", " saaxxxx   ");
    }

    @Test
    public void testLeft() throws Exception {
        checkQuery("SELECT Left ('Found on the Net', 4),Left (null, 4) FROM t234", "Foun", null);
    }

    @Test
    public void testLen() throws Exception {
        checkQuery("SELECT len('1222sssss.3hhh'),len(null) FROM t234", 14, null);
    }

    @Test
    public void testLTrim() throws Exception {
        checkQuery("SELECT LTRIM(' SSS   ') FROM t234", "SSS   ");
    }

    @Test
    public void testMid() throws Exception {
        checkQuery(
                "SELECT Mid ('Found on the Net', 2, 4), Mid ('Found on the Net', 1, 555),Mid(null, 1, 555) FROM t234",
                "ound", "Found on the Net", null);
    }

    @Test
    public void testMinute() throws Exception {
        checkQuery("SELECT Minute(#10:42:58 pM#),Minute(#10:42:58 AM#),Minute(#11/22/2003 10:42:58 PM#) FROM t234", 42,
                42, 42);
    }

    @Test
    public void testMonth() throws Exception {
        checkQuery("SELECT month(#11/22/2003 10:42:58 PM#) FROM t234", 11);
    }

    @Test
    public void testNow() throws Exception {
        Calendar cl = Calendar.getInstance();
        cl.set(Calendar.MILLISECOND, 0);
        checkQuery("select  now() FROM t234", cl.getTime());
    }

    @Test
    public void testTime() throws Exception {
        Calendar cl = Calendar.getInstance();
        cl.set(Calendar.MILLISECOND, 0);
        cl.set(1899, 11, 30);
        checkQuery("select  time() FROM t234", cl.getTime());
    }

    @Test
    public void testReplace() throws Exception {
        checkQuery("SELECT Replace('alphabet', 'bet', 'hydr') FROM t234", "alphahydr");
    }

    @Test
    public void testRight() throws Exception {
        checkQuery("SELECT Right ('Tech on the Net', 3),Right(null,12) FROM t234", "Net", null);
    }

    @Test
    public void testRtrim() throws Exception {
        checkQuery("SELECT RTRIM(' SSS   ') FROM t234", " SSS");
    }

    @Test
    public void testSecond() throws Exception {
        checkQuery("SELECT Second(#10:42:58 pM#),Second(#10:42:58 AM#),Second(#11/22/2003 10:42:58 PM#) FROM t234", 58,
                58, 58);
    }

    @Test
    public void testSin() throws Exception {
        checkQuery("SELECT sin(1) FROM t234", 0.8414709848078965);
    }

    @Test
    public void testSpace() throws Exception {
        checkQuery("SELECT space(5) FROM t234", "     ");
    }

    @Test
    public void testTrim() throws Exception {
        checkQuery("SELECT TRIM(' SSS   ') FROM t234", "SSS");
    }

    @Test
    public void testUcase() throws Exception {
        checkQuery("SELECT UCASE(' SAAxxxx   ') FROM t234", " SAAXXXX   ");
    }

    @Test
    public void testVal() throws Exception {
        checkQuery(
                "SELECT val('0.'), Val('hhh'),val('.a'),val('.') ,val('.44'), Val('1222.3hhh'),Val('12 22.3hhh'),VAL('-'),VAL('-2,3') FROM t234",
                0.0, 0.0, 0.0, 0.0, 0.44, 1222.3, 1222.3, 0.0, -2.0);
    }

    @Test
    public void testYear() throws Exception {
        checkQuery("SELECT year(#11/22/2003 10:42:58 PM#) FROM t234", 2003);
    }

    @Test
    public void testDateDiff() throws Exception {
        checkQuery("SELECT dateDiff('yyyy',#11/22/1992 10:42:58 PM#,#11/22/2007 10:42:58 AM#) FROM t234", 15);
        checkQuery("SELECT dateDiff('y',#11/22/1992 11:00:00 AM#,#11/22/2007 10:00:00 AM#) FROM t234", 5478);
        checkQuery("SELECT dateDiff('y',#11/22/1992 11:00:00 AM#,#11/22/2007 12:00:00 AM#) FROM t234", 5478);
        checkQuery("SELECT dateDiff('yyyy',#11/22/1992 10:42:58 PM#,#11/22/2007 10:42:58 AM#) FROM t234", 15);
        checkQuery("SELECT dateDiff('m',#11/22/2007 11:00:00 AM#,#11/22/2007 10:00:00 AM#) FROM t234", 0);
        checkQuery("SELECT dateDiff('m',#11/22/1992 11:00:00 AM#,#08/22/2007 12:00:00 AM#) FROM t234", 177);
        checkQuery("SELECT dateDiff('d',#1/1/2004 11:00:00 AM#,#1/3/2004 11:00:00 AM#) FROM t234", 2);
        checkQuery("SELECT dateDiff('d',#11/22/1992 11:00:00 AM#,#11/22/2007 10:00:00 AM#) FROM t234", 5478);
        checkQuery("SELECT dateDiff('d',#11/22/1992 11:00:00 AM#,#11/22/2007 12:00:00 AM#) FROM t234", 5478);
        checkQuery("SELECT dateDiff('w',#11/22/1992 11:00:00 AM#,#11/22/2007 10:00:00 AM#) FROM t234", 782);
        checkQuery("SELECT dateDiff('w',#11/22/1992 11:00:00 AM#,#11/22/2007 12:00:00 AM#) FROM t234", 782);
        checkQuery("SELECT dateDiff('ww',#11/22/1992 11:00:00 AM#,#11/22/2007 10:00:00 AM#) FROM t234", 782);
        checkQuery("SELECT dateDiff('ww',#11/22/1992 11:00:00 AM#,#11/22/2007 12:00:00 AM#) FROM t234", 782);
        checkQuery("SELECT dateDiff('ww',#10/22/2007 11:00:00 AM#,#11/22/2007 10:00:00 AM#) FROM t234", 4);
        checkQuery("SELECT dateDiff('ww',#07/22/2007 11:00:00 AM#,#11/22/2007 12:00:00 AM#) FROM t234", 17);
        checkQuery("SELECT dateDiff('h',#10/22/2007 08:01:00 AM#,#10/22/2006 04:00:00 AM#) FROM t234", -8764);
        checkQuery("SELECT dateDiff('h',#10/22/2007 10:07:00 AM#,#10/22/2007 11:07:00 AM#) FROM t234", 1);
        checkQuery("SELECT dateDiff('h',#10/22/2007 11:00:00 AM#,#10/22/2007 10:07:00 AM#) FROM t234", -1);

        checkQuery("SELECT dateDiff('n',#10/22/2007 08:00:00 AM#,#10/22/2003 04:00:00 AM#) FROM t234", -2104080);
        checkQuery("SELECT dateDiff('h',#10/22/2007 08:00:00 AM#,#10/22/2005 04:00:00 AM#) FROM t234", -17524);
    }

    @Test
    public void testDatePart() throws Exception {
        checkQuery("SELECT datePart('yyyy',#11/22/1992 10:42:58 PM#) FROM t234", 1992);
        checkQuery("SELECT datePart('q',#11/22/1992 10:42:58 PM#) FROM t234", 4);
        checkQuery("SELECT datePart('d',#11/22/1992 10:42:58 PM#) FROM t234", 22);
        checkQuery("SELECT datePart('y',#11/22/1992 10:42:58 PM#) FROM t234", 327);
        checkQuery("SELECT datePart('ww',#11/22/1992 10:42:58 PM#) FROM t234", 48);
        checkQuery("SELECT datePart('ww',#11/22/2006 10:42:58 PM#,3) FROM t234", 48);
        checkQuery("SELECT datePart('w',#05/8/2013#,7), datePart('ww',#11/22/2006 10:42:58 PM#,6,3) FROM t234", 5, 46);
        checkQuery("SELECT datePart('w',#05/13/1992 10:42:58 PM#) FROM t234", 4);
        checkQuery("SELECT datePart('h',#05/13/1992 10:42:58 PM#) FROM t234", 22);
        checkQuery("SELECT datePart('n',#05/13/1992 10:42:58 PM#) FROM t234", 42);
        checkQuery("SELECT datePart('s',#05/13/1992 10:42:58 PM#) FROM t234", 58);

    }

    @Test
    public void testDateSerial() throws Exception {
        checkQuery("SELECT dateserial(1998,5, 10) FROM t234", SDF.parse("1998-05-10 00:00:00"));
        checkQuery("SELECT 'It works, I can''t believe it.' FROM t234 WHERE #05/13/1992#=dateserial(1992,05,13)",
                "It works, I can't believe it.");
    }

    @Test
    public void testFormatNumber() throws Exception {
        checkQuery("SELECT format(0.981,'percent') FROM t234", "98.10%");
        checkQuery("SELECT format(num,'fixed')  FROM t234", "-1110.55");
        checkQuery("SELECT format(num,'standard')  FROM t234", "-1,110.55");
        checkQuery("SELECT format(num,'general number')  FROM t234", "-1110.554");
        checkQuery("SELECT format(num,'on/off') FROM t234", "On");
        checkQuery("SELECT format(num,'true/false') FROM t234", "True");
        checkQuery("SELECT format(num,'yes/no') FROM t234", "Yes");
        checkQuery("SELECT Format (11111210.6, '#,##0.00') FROM t234", "11,111,210.60");
        checkQuery("SELECT Format (1111111210.6, 'Scientific') FROM t234", "1.11E+09");
        checkQuery("SELECT Format (0.00000000000000015661112106, 'Scientific') FROM t234", "1.57E-16");
    }

    @Test
    public void testTimestamp() throws Exception {
        checkQuery("SELECT #2006-12-11#=timestamp '2006-12-11 00:00:00' FROM dual", true);
        checkQuery("SELECT #2006-12-11 1:2:3#=timestamp '2006-12-11 01:02:03' FROM dual", true);
        checkQuery("SELECT #2006-2-1 1:2:3#=timestamp '2006-02-01 01:02:03' FROM dual", true);
        checkQuery("SELECT #2/1/2006 1:2:3#=timestamp '2006-02-01 01:02:03' FROM dual", true);
        checkQuery("SELECT #12/11/2006 1:2:3#=timestamp '2006-12-11 01:02:03' FROM dual", true);
        checkQuery("SELECT #1392-01-10 1:2:3#=timestamp '1392-01-02 01:02:03' FROM dual", true);

        checkQuery("SELECT #12/11/2006 1:2:3 am#=timestamp '2006-12-11 01:02:03' FROM dual", true);
        checkQuery("SELECT #12/11/2006 1:2:3 pm#=timestamp '2006-12-11 13:02:03' FROM dual", true);
    }

    @Test
    public void testFormatDate() throws Exception {
        checkQuery("SELECT format(#05/13/1994 10:42:58 PM#,'Long date') FROM t234", "Friday, May 13, 1994");

        checkQuery("SELECT format(#05/13/1994 10:42:58 PM#,'Short date') FROM t234", "5/13/1994");
        checkQuery("SELECT format(#05/13/1994 10:42:58 AM#,'Long time') FROM t234", "10:42:58 AM");

        checkQuery("SELECT format(#05/13/1994 10:42:58 PM#,'Short time') FROM t234", "22:42");
        checkQuery("SELECT format(#05/13/1994 10:42:58 PM#,'General date') FROM t234", "5/13/1994 10:42:58 PM");

        checkQuery("SELECT format(#05/13/1994 10:42:58 PM#,'Medium date') FROM t234", "13-May-94");

        checkQuery("SELECT format(#05/13/1994 10:42:18 PM#,'Medium time') FROM t234", "10:42 PM");
    }

    @Test
    public void testSign() throws Exception {
        checkQuery("SELECT sign (0),sign(-20.4),sign(4)from t234", 0, -1, 1);

    }

    @Test
    public void testWeekDayName() throws Exception {
        checkQuery("SELECT weekDayName(3) FROM t234", "Tuesday");
        checkQuery("SELECT weekDayName(3,true) FROM t234", "Tue");
        checkQuery("select  WeekdayName (3, TRUE, 2) FROM t234", "Wed");
        dumpQueryResult("select  WeekdayName(Weekday(#2001-1-1#)) FROM t234");
    }

    @Test
    public void testMonthName() throws Exception {
        checkQuery("SELECT MonthName(3) FROM t234", "March");
        checkQuery("SELECT MonthName(3, true) FROM t234", "Mar");
    }

    @Test
    public void testStr() throws Exception {
        checkQuery("SELECT str(id), str(num), str(4.5555555) FROM t234", " 1234", "-1110.554", " 4.5555555");
    }

    @Test
    public void testDateValue() throws Exception {
        checkQuery("SELECT dateValue(#11/22/2003 10:42:58 PM#) FROM t234",
                SDF.parse("2003-11-22 00:00:00.0"));
        checkQuery("SELECT dateValue(#11/22/2003 21:42:58 AM#) FROM t234",
                SDF.parse("2003-11-22 00:00:00.0"));
        checkQuery("SELECT dateValue('6/30/2004') FROM t234", SDF.parse("2004-06-30 00:00:00.0"));
    }

    @Test
    public void testFormatString() throws Exception {
        checkQuery("SELECT format('05/13/1994','Long date') FROM t234", "Friday, May 13, 1994");
        checkQuery("SELECT format(0.6,'percent') FROM t234", "60.00%");
        checkQuery("SELECT format('0,6','percent') FROM t234", "600.00%");
        // beware of bug http://bugs.java.com/view_bug.do?bug_id=7131459 !
        checkQuery("SELECT format(48.14251,'.###') FROM t234", "48.143");
    }

    @Test
    public void testInt() throws Exception {
        checkQuery("SELECT int(1111112.5), int(-2.5)from t234", 1111112, -3);
    }

    @Test
    public void testRnd() throws Exception {
        dumpQueryResult("SELECT rnd()from t234");
    }

    @Test
    public void testStrComp() throws Exception {
        checkQuery("SELECT StrComp('Cia','Cia') FROM t234", 0);
    }

    @Test
    public void testStrConv() throws Exception {
        checkQuery("SELECT StrConv('Cia',1) FROM t234", "CIA");
    }

    @Test
    public void testStrReverse() throws Exception {
        checkQuery("SELECT StrReverse('ylatI') FROM t234", "Italy");
    }

    @Test
    public void testString() throws Exception {
        checkQuery("SELECT String(4,'c') FROM t234", "cccc");
    }

    @Test
    public void testWeekday() throws Exception {
        checkQuery("SELECT Weekday(#06/27/2013 10:42:58 PM#,1) FROM t234", 5);
    }

    @Test
    public void testFinancial() throws Exception {
        checkQuery(
                "SELECT FV(0,100,-100,-10000,-1), DDB(1001100,10020,111,62,5.5), NPer(0.0525,200,1500,233,0.1), IPmt(0.5,4,8,10*1,10000,0.5), PV(0,4,-10000,1000,-1.55),PPmt(0.5,3,7,100000,15000.1),SLN(10000,110000,9),SYD(10000,200,12,4),Pmt(0.08,30,5000,-15000,0.1) FROM t234",
                20000.0, 2234.68083152805, -7.721791247488574, 477.63917525773195, 39000.0, -8042.7461874696455,
                -11111.111111111111, 1130.7692307692307, -311.72566612727735);
        checkQuery("SELECT Rate(3,200,-610,0,-20,0.1) FROM t234", -0.01630483472667564);
    }
}
