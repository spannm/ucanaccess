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
package net.ucanaccess.test;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.Calendar;
import java.util.Locale;

import com.healthmarketscience.jackcess.Database.FileFormat;

public class FunctionsTest extends UcanaccessTestBase {
    private static boolean                init;
    private final static SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    public FunctionsTest() {
        super();
        Locale.setDefault(Locale.US);
    }

    public FunctionsTest(FileFormat accVer) {
        super(accVer);
        Locale.setDefault(Locale.US);
    }

    @Override
    protected void setUp() throws Exception {

        super.setUp();
        if (!init) {
            Statement st = null;

            st = super.ucanaccess.createStatement();
            st.executeUpdate("CREATE TABLE t234 (id INTEGER,descr text(400), num numeric(12,3), date0 datetime) ");
            st.close();
            st = super.ucanaccess.createStatement();
            st.execute(
                    "INSERT INTO t234 (id,descr,num,date0)  VALUES( 1234,'Show must go off',-1110.55446,#11/22/2003 10:42:58 PM#)");

            st.close();
            init = true;
        }

    }

    public void testASC() throws SQLException, IOException {
        checkQuery("select  ASC('A') FROM t234", 65);
        checkQuery("select  ASC('1') FROM t234", 49);
        checkQuery("select  ASC('u') FROM t234", 117);
    }

    public void testSwitch() throws SQLException, IOException {
        checkQuery("select  switch('1'='1',1,false,2,true, 1 )FROM t234");

    }

    public void testATN() throws SQLException, IOException {
        checkQuery("select atn(3) FROM t234", 1.2490457723982544);
    }

    public void testNz() throws SQLException, IOException {
        checkQuery("select nz(null,'lampredotto'),nz('turtelaz','lampredotto'), nz(null, 1.5),nz(2, 2)  FROM t234",
                "lampredotto", "turtelaz", 1.5, 2);
    }

    public void testCBoolean() throws SQLException, IOException {
        checkQuery("select cbool(id),cbool(1=2),cbool('true'),cbool('false'),cbool(0),cbool(-3) from t234 ",
                new Object[][] { { true, false, true, false, false, true } });
    }

    public void testCVar() throws SQLException, IOException {
        checkQuery("select cvar(8),cvar(8.44) from t234 ", "8", "8.44");
    }

    public void testCstr() throws SQLException, IOException, ParseException {
        checkQuery("select cstr(date0) from t234 ", "11/22/2003 10:42:58 PM");
        checkQuery("select cstr(false) from t234 ", "false");
        checkQuery("select cstr(8) from t234 ", "8");
        checkQuery("select cstr(8.78787878) from t234 ", "8.78787878");

    }

    public void testCsign() throws SQLException, IOException, ParseException {
        checkQuery("select csign(8.53453543) from t234 ", 8.534535);
    }

    public void testCDate() throws SQLException, IOException, ParseException {
        checkQuery("select  Cdate('Apr 6, 2003')  from t234  ", SDF.parse("2003-04-06 00:00:00.0"));

        checkQuery("select  Cdate('1582-10-15')  from t234  ", SDF.parse("1582-10-15 00:00:00.0"));
    }

    public void testCLong() throws SQLException, IOException, ParseException {
        checkQuery("select  Clong(8.52), Clong(8.49 ),Clong(5.5)  from t234  ", 9, 8, 6);
    }

    public void testCLng() throws SQLException, IOException, ParseException {
        checkQuery("select  Clng(8.52), Clng(8.49 ),Clng(5.5)  from t234  ", 9, 8, 6);
    }

    public void testCDec() throws SQLException, IOException, ParseException {
        checkQuery("select cdec(8.45 * 0.005 * 0.01) from t234  ", 0.0004225);
    }

    public void testCcur() throws SQLException, IOException, ParseException {
        checkQuery("select  Ccur(123.4567812),  Ccur(123.4547812)  from t234  ", 123.4568, 123.4548);

        checkQuery("select ccur(0.552222211)*100  from t234  ", 55.22);
    }

    public void testCint() throws SQLException, IOException, ParseException {
        checkQuery("select  Cint(8.51), Cint(4.5) from t234  ", 9, 4);
    }

    public void testChr() throws SQLException, IOException {
        checkQuery("select  CHR(65) FROM t234", "A");
    }

    public void testCos() throws SQLException, IOException {
        checkQuery("select cos(1) FROM t234", 0.5403023058681398);
    }

    public void testCurrentUser() throws SQLException, IOException {
        checkQuery("select CurrentUser() from t234", "ucanaccess");
    }

    public void testDateAdd() throws SQLException, IOException, ParseException {

        checkQuery("select dateAdd('YYYY',4 ,#11/22/2003 10:42:58 PM#)FROM t234", SDF.parse("2007-11-22 22:42:58"));
        checkQuery("select dateAdd('Q',3 ,#11/22/2003 10:42:58 PM#)FROM t234", SDF.parse("2004-08-22 22:42:58"));
        checkQuery("select dateAdd('Y',451 ,#11/22/2003 10:42:58 PM#)FROM t234", SDF.parse("2005-02-15 22:42:58"));
        checkQuery("select dateAdd('D',451 ,#11/22/2003 10:42:58 PM#)FROM t234", SDF.parse("2005-02-15 22:42:58"));
        checkQuery("select dateAdd('Y',45 ,#11/22/2003 10:42:58 PM#)FROM t234", SDF.parse("2004-01-06 22:42:58"));
        checkQuery("select dateAdd('D',45 ,#11/22/2003 10:42:58 PM#)FROM t234", SDF.parse("2004-01-06 22:42:58"));
        checkQuery("select dateAdd('Y',4 ,#11/22/2003 10:42:58 PM#)FROM t234", SDF.parse("2003-11-26 22:42:58"));
        checkQuery("select dateAdd('D',4 ,#11/22/2003 10:42:58 PM#)FROM t234", SDF.parse("2003-11-26 22:42:58"));
        checkQuery("select dateAdd('W',43 ,#11/22/2003 10:42:58 PM#)FROM t234", SDF.parse("2004-01-04 22:42:58"));
        checkQuery("select dateAdd('W',1 ,#11/22/2003 10:42:58 PM#)FROM t234", SDF.parse("2003-11-23 22:42:58"));
        checkQuery("select dateAdd('WW',43 ,#11/22/2003 10:42:58 PM#)FROM t234", SDF.parse("2004-09-18 22:42:58"));
        checkQuery("select dateAdd('H',400 ,#11/22/2003 10:42:58 PM#)FROM t234", SDF.parse("2003-12-09 14:42:58"));
        checkQuery("select dateAdd('M',400 ,#11/22/2003 10:42:58 PM#)FROM t234", SDF.parse("2037-03-22 22:42:58"));
        checkQuery("select dateAdd('S',400 ,#11/22/2003 10:42:58 PM#)FROM t234", SDF.parse("2003-11-22 22:49:38"));
    }

    public void testDate() throws SQLException, IOException {
        checkQuery("select date() FROM t234");
    }

    public void testDay() throws SQLException, IOException {
        checkQuery("select day(#11/22/2003 10:42:58 PM#)FROM t234", 22);
    }

    public void testExp() throws SQLException, IOException {
        checkQuery("select exp(3.1),exp(0.4) from t234 ", 22.197951281441636, 1.4918246976412703);
    }

    public void testHour() throws SQLException, IOException {
        checkQuery("select Hour(#10:42:58 pM#),Hour(#10:42:58 AM#),Hour(#11/22/2003 10:42:58 PM#) FROM t234", 22, 10,
                22);
    }

    public void testIif() throws SQLException, IOException {
        checkQuery(
                "select  IIf(isNull(descr)=true,'pippo','pl''uto'&'\" \" cccc'),IIf(isNull(descr)=true,'pippo','pl''uto'&'\" \" cccc') from t234 ",
                "pl'uto\" \" cccc", "pl'uto\" \" cccc");

        checkQuery("select  IIf(true,false,true) from t234 ", false);
        checkQuery("select  IIf('pippo'=null,'capra','d''una capra') from t234 ", "d'una capra");

    }

    public void testInstr() throws SQLException, IOException {
        checkQuery("select Instr ( 'Found on the Net', 'the') FROM t234", 10);
        checkQuery("select Instr ( 'Found on the Net', 'f') FROM t234", 1);
        checkQuery("select Instr ( 1,'Found on the Net', 'f') FROM t234", 1);
        checkQuery("select Instr ( 1,'Found on the Net', 'f',1) FROM t234", 1);
        checkQuery("select Instr ( 1,'Found on the Net', 't',0) FROM t234", 10);
        checkQuery("select Instr ( 31,'Found on the Net', 'f',0) FROM t234", 0);
    }

    public void testInstrrev() throws SQLException, IOException {
        checkQuery("select InstrRev ('alphabet', 'a') FROM t234", 5);
        checkQuery("select InstrRev ('alphabet', 'a',-1) FROM t234", 5);
        checkQuery("select InstrRev ('alphabet', 'a',1) FROM t234", 1);
    }

    public void testIsDate() throws SQLException, IOException {
        checkQuery("select isDate(#1/2/2003 10:42:58 PM#) from t234", true);
        checkQuery("select isDate(#11/22/2003 10:42:58 PM#) from t234", true);
        checkQuery("select isDate('11/22/2003 10:42:58 PM') from t234", true);
        checkQuery("select isDate('january 3,2015') from t234 ", true);
        checkQuery("select isDate('janr 3,2015') from t234 ", false);
        checkQuery("select isDate('03 3,2015') from t234 ", true);
        checkQuery("select isDate('3 3,2015') from t234 ", true);
        checkQuery("select isDate('Fri Feb 10 00:25:09 CET 2012') from t234 ", false);
        checkQuery("select isDate('Fri Feb 10 2012') from t234 ", false);
        checkQuery("select isDate('Fri Feb 10 00:25:09 2012') from t234 ", false);
        checkQuery("select isDate('Fri Feb 10 00:25:09') from t234 ", false);
        checkQuery("select isDate('Feb 10 00:25:09') from t234 ", true);
        checkQuery("select isDate('02 10 00:25:09') from t234 ", true);
        checkQuery("select isDate('Feb 35 00:25:09') from t234 ", true);
        checkQuery("select isDate('jan 35,2015') from t234 ", false);
        checkQuery("select isDate('Feb 20 01:25:09 PM') from t234 ", true);
    }

    public void testIsNumber() throws SQLException, IOException {
        checkQuery("select isNumeric(33)   from t234 ", true);
        checkQuery("select isNumeric('33') from t234 ", true);
        checkQuery("select isNumeric('a')  from t234 ", false);
        checkQuery("select isNumeric('33d')from t234 ", false);
        checkQuery("select isNumeric(id)   from t234 ", true);
        checkQuery("select isNumeric('4,5')   from t234 ", true);
    }

    public void testLcase() throws SQLException, IOException {
        checkQuery("select LCASE(' SAAxxxx   ') FROM t234", " saaxxxx   ");
    }

    public void testLeft() throws SQLException, IOException {
        checkQuery("select Left ('Found on the Net', 4),Left (null, 4) from t234 ", "Foun", null);
    }

    public void testLen() throws SQLException, IOException {
        checkQuery("select len('1222sssss.3hhh'),len(null) from t234 ", 14, null);
    }

    public void testLTrim() throws SQLException, IOException {
        checkQuery("select LTRIM(' SSS   ') FROM t234", "SSS   ");
    }

    public void testMid() throws SQLException, IOException {
        checkQuery(
                "select Mid ('Found on the Net', 2, 4), Mid ('Found on the Net', 1, 555),Mid(null, 1, 555) from t234 ",
                "ound", "Found on the Net", null);
    }

    public void testMinute() throws SQLException, IOException {
        checkQuery("select Minute(#10:42:58 pM#),Minute(#10:42:58 AM#),Minute(#11/22/2003 10:42:58 PM#)FROM t234", 42,
                42, 42);
    }

    public void testMonth() throws SQLException, IOException {
        checkQuery("select month(#11/22/2003 10:42:58 PM#)FROM t234", 11);
    }

    public void testNow() throws SQLException, IOException {
        Calendar cl = Calendar.getInstance();
        cl.set(Calendar.MILLISECOND, 0);
        checkQuery("select  now() from t234 ", cl.getTime());

    }

    public void testTime() throws SQLException, IOException {
        Calendar cl = Calendar.getInstance();
        cl.set(Calendar.MILLISECOND, 0);
        cl.set(1899, 11, 30);
        checkQuery("select  time() from t234 ", cl.getTime());
    }

    public void testReplace() throws SQLException, IOException {
        checkQuery("select Replace('alphabet', 'bet', 'hydr') from t234 ", "alphahydr");
    }

    public void testRight() throws SQLException, IOException {
        checkQuery("select Right ('Tech on the Net', 3),Right(null,12) from t234 ", "Net", null);
    }

    public void testRtrim() throws SQLException, IOException {
        checkQuery("select RTRIM(' SSS   ') FROM t234", " SSS");
    }

    public void testSecond() throws SQLException, IOException {
        checkQuery("select Second(#10:42:58 pM#),Second(#10:42:58 AM#),Second(#11/22/2003 10:42:58 PM#)FROM t234", 58,
                58, 58);
    }

    public void testSin() throws SQLException, IOException {
        checkQuery("select sin(1) FROM t234", 0.8414709848078965);
    }

    public void testSpace() throws SQLException, IOException {
        checkQuery("select space(5) FROM t234", "     ");
    }

    public void testTrim() throws SQLException, IOException {
        checkQuery("select TRIM(' SSS   ') FROM t234", "SSS");
    }

    public void testUcase() throws SQLException, IOException {
        checkQuery("select UCASE(' SAAxxxx   ') FROM t234", " SAAXXXX   ");
    }

    public void testVal() throws SQLException, IOException {
        checkQuery(
                "select val('0.'), Val('hhh'),val('.a'),val('.') ,val('.44'), Val('1222.3hhh'),Val('12 22.3hhh'),VAL('-'),VAL('-2,3') from t234 ",
                0.0, 0.0, 0.0, 0.0, 0.44, 1222.3, 1222.3, 0.0, -2.0);
    }

    public void testYear() throws SQLException, IOException {
        checkQuery("select year(#11/22/2003 10:42:58 PM#)FROM t234", 2003);
    }

    public void testDateDiff() throws SQLException, IOException {
        checkQuery("select dateDiff('yyyy',#11/22/1992 10:42:58 PM#,#11/22/2007 10:42:58 AM#)FROM t234", 15);
        checkQuery("select dateDiff('y',#11/22/1992 11:00:00 AM#,#11/22/2007 10:00:00 AM#)FROM t234", 5478);
        checkQuery("select dateDiff('y',#11/22/1992 11:00:00 AM#,#11/22/2007 12:00:00 AM#)FROM t234", 5478);
        checkQuery("select dateDiff('yyyy',#11/22/1992 10:42:58 PM#,#11/22/2007 10:42:58 AM#)FROM t234", 15);
        checkQuery("select dateDiff('m',#11/22/2007 11:00:00 AM#,#11/22/2007 10:00:00 AM#)FROM t234", 0);
        checkQuery("select dateDiff('m',#11/22/1992 11:00:00 AM#,#08/22/2007 12:00:00 AM#)FROM t234", 177);
        checkQuery("select dateDiff('d',#1/1/2004 11:00:00 AM#,#1/3/2004 11:00:00 AM#)FROM t234", 2);
        checkQuery("select dateDiff('d',#11/22/1992 11:00:00 AM#,#11/22/2007 10:00:00 AM#)FROM t234", 5478);
        checkQuery("select dateDiff('d',#11/22/1992 11:00:00 AM#,#11/22/2007 12:00:00 AM#)FROM t234", 5478);
        checkQuery("select dateDiff('w',#11/22/1992 11:00:00 AM#,#11/22/2007 10:00:00 AM#)FROM t234", 782);
        checkQuery("select dateDiff('w',#11/22/1992 11:00:00 AM#,#11/22/2007 12:00:00 AM#)FROM t234", 782);
        checkQuery("select dateDiff('ww',#11/22/1992 11:00:00 AM#,#11/22/2007 10:00:00 AM#)FROM t234", 782);
        checkQuery("select dateDiff('ww',#11/22/1992 11:00:00 AM#,#11/22/2007 12:00:00 AM#)FROM t234", 782);
        checkQuery("select dateDiff('ww',#10/22/2007 11:00:00 AM#,#11/22/2007 10:00:00 AM#)FROM t234", 4);
        checkQuery("select dateDiff('ww',#07/22/2007 11:00:00 AM#,#11/22/2007 12:00:00 AM#)FROM t234", 17);
        checkQuery("select dateDiff('h',#10/22/2007 08:01:00 AM#,#10/22/2006 04:00:00 AM#)FROM t234", -8764);
        checkQuery("select dateDiff('h',#10/22/2007 10:07:00 AM#,#10/22/2007 11:07:00 AM#)FROM t234", 1);
        checkQuery("select dateDiff('h',#10/22/2007 11:00:00 AM#,#10/22/2007 10:07:00 AM#)FROM t234", -1);

        checkQuery("select dateDiff('n',#10/22/2007 08:00:00 AM#,#10/22/2003 04:00:00 AM#)FROM t234", -2104080);
        checkQuery("select dateDiff('h',#10/22/2007 08:00:00 AM#,#10/22/2005 04:00:00 AM#)FROM t234", -17524);
    }

    public void testDatePart() throws SQLException, IOException {
        checkQuery("select datePart('yyyy',#11/22/1992 10:42:58 PM#)FROM t234", 1992);
        checkQuery("select datePart('q',#11/22/1992 10:42:58 PM#)FROM t234", 4);
        checkQuery("select datePart('d',#11/22/1992 10:42:58 PM#)FROM t234", 22);
        checkQuery("select datePart('y',#11/22/1992 10:42:58 PM#)FROM t234", 327);
        checkQuery("select datePart('ww',#11/22/1992 10:42:58 PM#)FROM t234", 48);
        checkQuery("select datePart('ww',#11/22/2006 10:42:58 PM#,3)FROM t234", 48);
        checkQuery("select datePart('w',#05/8/2013#,7), datePart('ww',#11/22/2006 10:42:58 PM#,6,3)FROM t234 ", 5, 46);
        checkQuery("select datePart('w',#05/13/1992 10:42:58 PM#)FROM t234", 4);
        checkQuery("select datePart('h',#05/13/1992 10:42:58 PM#)FROM t234", 22);
        checkQuery("select datePart('n',#05/13/1992 10:42:58 PM#)FROM t234", 42);
        checkQuery("select datePart('s',#05/13/1992 10:42:58 PM#)FROM t234", 58);

    }

    public void testDateSerial() throws SQLException, IOException, ParseException {
        checkQuery("select dateserial(1998,5, 10)FROM t234", SDF.parse("1998-05-10 00:00:00"));
        checkQuery("select 'It works, I can''t believe it.' FROM t234 WHERE #05/13/1992#=dateserial(1992,05,13)",
                "It works, I can't believe it.");
    }

    public void testFormatNumber() throws SQLException, IOException {
        checkQuery("select format(0.981,'percent') from t234", "98.10%");
        checkQuery("select format(num,'fixed')   from t234", "-1110.55");
        checkQuery("select format(num,'standard')   from t234", "-1,110.55");
        checkQuery("select format(num,'general number')   from t234", "-1110.554");
        checkQuery("select format(num,'on/off')  from t234", "On");
        checkQuery("select format(num,'true/false') from t234", "True");
        checkQuery("select format(num,'yes/no')  from t234", "Yes");
        checkQuery("select Format (11111210.6, '#,##0.00') from t234", "11,111,210.60");
        checkQuery("select Format (1111111210.6, 'Scientific') from t234", "1.11E+09");
        checkQuery("select Format (0.00000000000000015661112106, 'Scientific') from t234", "1.57E-16");
    }

    public void testTimestamp() throws SQLException, IOException {
        checkQuery("select #2006-12-11#=timestamp '2006-12-11 00:00:00' from dual", true);
        checkQuery("select #2006-12-11 1:2:3#=timestamp '2006-12-11 01:02:03' from dual", true);
        checkQuery("select #2006-2-1 1:2:3#=timestamp '2006-02-01 01:02:03' from dual", true);
        checkQuery("select #2/1/2006 1:2:3#=timestamp '2006-02-01 01:02:03' from dual", true);
        checkQuery("select #12/11/2006 1:2:3#=timestamp '2006-12-11 01:02:03' from dual", true);
        checkQuery("select #1392-01-10 1:2:3#=timestamp '1392-01-02 01:02:03' from dual", true);

        checkQuery("select #12/11/2006 1:2:3 am#=timestamp '2006-12-11 01:02:03' from dual", true);
        checkQuery("select #12/11/2006 1:2:3 pm#=timestamp '2006-12-11 13:02:03' from dual", true);

    }

    public void testFormatDate() throws SQLException, IOException {

        checkQuery("select format(#05/13/1994 10:42:58 PM#,'Long date') from t234", "Friday, May 13, 1994");

        checkQuery("select format(#05/13/1994 10:42:58 PM#,'Short date') from t234", "5/13/1994");
        checkQuery("select format(#05/13/1994 10:42:58 AM#,'Long time') from t234", "10:42:58 AM");

        checkQuery("select format(#05/13/1994 10:42:58 PM#,'Short time') from t234", "22:42");
        checkQuery("select format(#05/13/1994 10:42:58 PM#,'General date') from t234", "5/13/1994 10:42:58 PM");

        checkQuery("select format(#05/13/1994 10:42:58 PM#,'Medium date') from t234", "13-May-94");

        checkQuery("select format(#05/13/1994 10:42:18 PM#,'Medium time') from t234", "10:42 PM");

    }

    public void testSign() throws SQLException, IOException {
        checkQuery("select sign (0),sign(-20.4),sign(4)from t234", 0, -1, 1);

    }

    public void testWeekDayName() throws SQLException, IOException {

        checkQuery("select weekDayName(3) from t234", "Tuesday");
        checkQuery("select weekDayName(3,true) from t234", "Tue");
        checkQuery("select  WeekdayName (3, TRUE, 2)  from t234", "Wed");
        dump("select  WeekdayName(Weekday(#2001-1-1#)) from t234");
    }

    public void testMonthName() throws SQLException, IOException {

        checkQuery("select MonthName(3) from t234", "March");
        checkQuery("select MonthName(3, true) from t234", "Mar");

    }

    public void testStr() throws SQLException, IOException {

        checkQuery("select str(id),str(num),str(4.5555555) from t234", " 1234", "-1110.554", " 4.5555555");

    }

    public void testDateValue() throws SQLException, IOException, ParseException {

        checkQuery("select dateValue(#11/22/2003 10:42:58 PM#) from t234", SDF.parse("2003-11-22 00:00:00.0"));
        checkQuery("select dateValue(#11/22/2003 21:42:58 AM#) from t234", SDF.parse("2003-11-22 00:00:00.0"));
        checkQuery("select dateValue('6/30/2004') from t234", SDF.parse("2004-06-30 00:00:00.0"));

    }

    public void testFormatString() throws SQLException, IOException, ParseException {
        checkQuery("select format('05/13/1994','Long date') from t234", "Friday, May 13, 1994");
        checkQuery("select format(0.6,'percent') from t234", "60.00%");
        checkQuery("select format('0,6','percent') from t234", "600.00%");
        checkQuery("select format(48.1425,'.###') from t234", "48.143");

    }

    public void testInt() throws SQLException, IOException {
        checkQuery("select int(1111112.5), int(-2.5)from t234", 1111112, -3);
    }

    public void testRnd() throws SQLException, IOException {
        dump("select rnd()from t234");
    }

    public void testStrComp() throws SQLException, IOException {
        checkQuery("select StrComp('Cia','Cia') from t234", 0);
    }

    public void testStrConv() throws SQLException, IOException {
        checkQuery("select StrConv('Cia',1) from t234", "CIA");
    }

    public void testStrReverse() throws SQLException, IOException {
        checkQuery("select StrReverse('ylatI') from t234", "Italy");
    }

    public void testString() throws SQLException, IOException {
        checkQuery("select String(4,'c') from t234", "cccc");
    }

    public void testWeekday() throws SQLException, IOException {
        checkQuery("select Weekday(#06/27/2013 10:42:58 PM#,1) from t234", 5);
    }

    public void testFinancial() throws SQLException, IOException {
        checkQuery(
                "select FV(0,100,-100,-10000,-1) ,DDB(1001100,10020,111,62,5.5),NPer(0.0525,200,1500,233,0.1),IPmt(0.5,4,8,10*1,10000,0.5),PV(0,4,-10000,1000,-1.55),PPmt(0.5,3,7,100000,15000.1),SLN(10000,110000,9),SYD(10000,200,12,4),Pmt(0.08,30,5000,-15000,0.1) from t234",
                20000.0, 2234.68083152805, -7.721791247488574, 477.63917525773195, 39000.0, -8042.7461874696455,
                -11111.111111111111, 1130.7692307692307, -311.72566612727735);
        checkQuery("select Rate(3,200,-610,0,-20,0.1) from t234", -0.01630483472667564);
    }
}
