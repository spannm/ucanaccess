/*
Copyright (c) 2012 Marco Amadei.

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
USA

You can contact Marco Amadei at amadei.mar@gmail.com.

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
	private static boolean init;
	private final static SimpleDateFormat SDF = new SimpleDateFormat(
			"yyyy-MM-dd hh:mm:ss");

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
			st
					.executeUpdate("CREATE TABLE t234 (id INTEGER,descr text(400), num numeric(12,3), date0 datetime) ");
			st.close();
			st = super.ucanaccess.createStatement();
			st
					.execute("INSERT INTO t234 (id,descr,num,date0)  VALUES( 1234,'Show must go off',-1110.55446,#11/22/2003 10:42:58 PM#)");

			st.close();
			init = true;
		}

	}

	public void testASC() throws SQLException, IOException {
		checkQuery("select  ASC('A') FROM t234", 65);
		checkQuery("select  ASC('1') FROM t234", 49);
		checkQuery("select  ASC('u') FROM t234", 117);
	}

	public void testATN() throws SQLException, IOException {
		checkQuery("select atn(3) FROM t234", 1.2490457723982544);
	}

	public void testNz() throws SQLException, IOException {
		checkQuery(
				"select nz(null,'lampredotto'),nz('turtelaz','lampredotto') FROM t234",
				"lampredotto", "turtelaz");
	}

	public void testCBoolean() throws SQLException, IOException {
		checkQuery(
				"select cbool(id),cbool(1=2),cbool('true'),cbool('false'),cbool(0),cbool(-3) from t234 ",
				new Object[][] { { true, false, true, false, false, true } });
	}

	public void testCVar() throws SQLException, IOException {
		checkQuery("select cvar(8),cvar(8.44) from t234 ", "8", "8,44");
	}

	public void testCstr() throws SQLException, IOException, ParseException {
		checkQuery("select cstr(date0) from t234 ", "22/11/2003 22:42:58");
		checkQuery("select cstr(false) from t234 ", "false");
		checkQuery("select cstr(8) from t234 ", "8");
		checkQuery("select cstr(8.78787878) from t234 ", "8.78787878");

	}

	public void testCsign() throws SQLException, IOException, ParseException {
		checkQuery("select csign(8.53453543) from t234 ", 8.534535);
	}

	public void testCDate() throws SQLException, IOException, ParseException {
		checkQuery("select  Cdate('Apr 6, 2003')  from t234  ", SDF
				.parse("2003-04-06 00:00:00.0"));
	}

	public void testCLong() throws SQLException, IOException, ParseException {
		checkQuery("select  Clong(8.52), Clong(8.49 ),Clong(5.5)  from t234  ",
				9, 8, 5);
	}

	public void testCDec() throws SQLException, IOException, ParseException {
		checkQuery("select cdec(8.45 * 0.005 * 0.01) from t234  ", 0.0004225);
	}

	public void testCcur() throws SQLException, IOException, ParseException {
		checkQuery(
				"select  Ccur(123.4567812),  Ccur(123.4547812)  from t234  ",
				123.4568,123.4548);
		
		checkQuery(
				"select ccur(0.552222211)*100  from t234  ",
				55.22);
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

		checkQuery(
				"select dateAdd('YYYY',4 ,#11/22/2003 10:42:58 PM#)FROM t234",
				SDF.parse("2007-11-22 22:42:58"));
		checkQuery("select dateAdd('Q',3 ,#11/22/2003 10:42:58 PM#)FROM t234",
				SDF.parse("2004-08-22 22:42:58"));
		checkQuery(
				"select dateAdd('Y',451 ,#11/22/2003 10:42:58 PM#)FROM t234",
				SDF.parse("2005-02-15 22:42:58"));
		checkQuery(
				"select dateAdd('D',451 ,#11/22/2003 10:42:58 PM#)FROM t234",
				SDF.parse("2005-02-15 22:42:58"));
		checkQuery("select dateAdd('Y',45 ,#11/22/2003 10:42:58 PM#)FROM t234",
				SDF.parse("2004-01-06 22:42:58"));
		checkQuery("select dateAdd('D',45 ,#11/22/2003 10:42:58 PM#)FROM t234",
				SDF.parse("2004-01-06 22:42:58"));
		checkQuery("select dateAdd('Y',4 ,#11/22/2003 10:42:58 PM#)FROM t234",
				SDF.parse("2003-11-26 22:42:58"));
		checkQuery("select dateAdd('D',4 ,#11/22/2003 10:42:58 PM#)FROM t234",
				SDF.parse("2003-11-26 22:42:58"));
		checkQuery("select dateAdd('W',43 ,#11/22/2003 10:42:58 PM#)FROM t234",
				SDF.parse("2004-01-04 22:42:58"));
		checkQuery("select dateAdd('W',1 ,#11/22/2003 10:42:58 PM#)FROM t234",
				SDF.parse("2003-11-23 22:42:58"));
		checkQuery(
				"select dateAdd('WW',43 ,#11/22/2003 10:42:58 PM#)FROM t234",
				SDF.parse("2004-09-18 22:42:58"));
		checkQuery(
				"select dateAdd('H',400 ,#11/22/2003 10:42:58 PM#)FROM t234",
				SDF.parse("2003-12-09 14:42:58"));
		checkQuery(
				"select dateAdd('M',400 ,#11/22/2003 10:42:58 PM#)FROM t234",
				SDF.parse("2037-03-22 22:42:58"));
		checkQuery(
				"select dateAdd('S',400 ,#11/22/2003 10:42:58 PM#)FROM t234",
				SDF.parse("2003-11-22 22:49:38"));
	}
	
	
	public void testDate() throws SQLException, IOException {
		 checkQuery("select date() FROM t234");
	}

	public void testDay() throws SQLException, IOException {
		checkQuery("select day(#11/22/2003 10:42:58 PM#)FROM t234", 22);
	}

	public void testExp() throws SQLException, IOException {
		checkQuery("select exp(3.1),exp(0.4) from t234 ", 22.197951281441636,
				1.4918246976412703);
	}

	public void testHour() throws SQLException, IOException {
		checkQuery(
				"select Hour(#10:42:58 pM#),Hour(#10:42:58 AM#),Hour(#11/22/2003 10:42:58 PM#) FROM t234",
				22, 10, 22);
	}

	public void testIif() throws SQLException, IOException {
		checkQuery(
				"select  IIf(isNull(descr)=true,'pippo','pl''uto'&'\" \" cccc'),IIf(isNull(descr)=true,'pippo','pl''uto'&'\" \" cccc') from t234 ",
				"pl'uto\" \" cccc", "pl'uto\" \" cccc");
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
		checkQuery("select isDate('Fri Feb 10 00:25:09 CET 2012') from t234 ",
				false);
		checkQuery("select isDate('Fri Feb 10 2012') from t234 ", false);
		checkQuery("select isDate('Fri Feb 10 00:25:09 2012') from t234 ",
				false);
		checkQuery("select isDate('Fri Feb 10 00:25:09') from t234 ", false);
		checkQuery("select isDate('Feb 10 00:25:09') from t234 ", true);
		checkQuery("select isDate('02 10 00:25:09') from t234 ", true);
		checkQuery("select isDate('Feb 35 00:25:09') from t234 ", true);
		checkQuery("select isDate('jan 35,2015') from t234 ", true);
		checkQuery("select isDate('Feb 20 01:25:09 PM') from t234 ", true);
	}

	public void testIsNumber() throws SQLException, IOException {
		checkQuery("select isNumeric(33)   from t234 ", true);
		checkQuery("select isNumeric('33') from t234 ", true);
		checkQuery("select isNumeric('a')  from t234 ", false);
		checkQuery("select isNumeric('33d')from t234 ", false);
		checkQuery("select isNumeric(id)   from t234 ", true);
	}

	public void testLcase() throws SQLException, IOException {
		checkQuery("select LCASE(' SAAxxxx   ') FROM t234", " saaxxxx   ");
	}

	public void testLeft() throws SQLException, IOException {
		checkQuery(
				"select Left ('Found on the Net', 4),Left (null, 4) from t234 ",
				"Foun", null);
	}

	public void testLen() throws SQLException, IOException {
		checkQuery("select len('1222sssss.3hhh'),len(null) from t234 ", 14,
				null);
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
		checkQuery(
				"select Minute(#10:42:58 pM#),Minute(#10:42:58 AM#),Minute(#11/22/2003 10:42:58 PM#)FROM t234",
				42, 42, 42);
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
		checkQuery("select Replace('alphabet', 'bet', 'hydr') from t234 ",
				"alphahydr");
	}

	public void testRigth() throws SQLException, IOException {
		checkQuery(
				"select Right ('Tech on the Net', 3),Right(null,12) from t234 ",
				"Net", null);
	}

	public void testRtrim() throws SQLException, IOException {
		checkQuery("select RTRIM(' SSS   ') FROM t234", " SSS");
	}

	public void testSecond() throws SQLException, IOException {
		checkQuery(
				"select Second(#10:42:58 pM#),Second(#10:42:58 AM#),Second(#11/22/2003 10:42:58 PM#)FROM t234",
				58, 58, 58);
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
		checkQuery(
				"select dateDiff('yyyy',#11/22/1992 10:42:58 PM#,#11/22/2007 10:42:58 AM#)FROM t234",
				15);
		checkQuery(
				"select dateDiff('y',#11/22/1992 11:00:00 AM#,#11/22/2007 10:00:00 AM#)FROM t234",
				5478);
		checkQuery(
				"select dateDiff('y',#11/22/1992 11:00:00 AM#,#11/22/2007 12:00:00 AM#)FROM t234",
				5478);
		checkQuery(
				"select dateDiff('yyyy',#11/22/1992 10:42:58 PM#,#11/22/2007 10:42:58 AM#)FROM t234",
				15);
		checkQuery(
				"select dateDiff('m',#11/22/2007 11:00:00 AM#,#11/22/2007 10:00:00 AM#)FROM t234",
				0);
		checkQuery(
				"select dateDiff('m',#11/22/1992 11:00:00 AM#,#08/22/2007 12:00:00 AM#)FROM t234",
				177);
		checkQuery(
				"select dateDiff('d',#1/1/2004 11:00:00 AM#,#1/3/2004 11:00:00 AM#)FROM t234",
				2);
		checkQuery(
				"select dateDiff('d',#11/22/1992 11:00:00 AM#,#11/22/2007 10:00:00 AM#)FROM t234",
				5478);
		checkQuery(
				"select dateDiff('d',#11/22/1992 11:00:00 AM#,#11/22/2007 12:00:00 AM#)FROM t234",
				5478);
		checkQuery(
				"select dateDiff('w',#11/22/1992 11:00:00 AM#,#11/22/2007 10:00:00 AM#)FROM t234",
				782);
		checkQuery(
				"select dateDiff('w',#11/22/1992 11:00:00 AM#,#11/22/2007 12:00:00 AM#)FROM t234",
				782);
		checkQuery(
				"select dateDiff('ww',#11/22/1992 11:00:00 AM#,#11/22/2007 10:00:00 AM#)FROM t234",
				782);
		checkQuery(
				"select dateDiff('ww',#11/22/1992 11:00:00 AM#,#11/22/2007 12:00:00 AM#)FROM t234",
				782);
		checkQuery(
				"select dateDiff('ww',#10/22/2007 11:00:00 AM#,#11/22/2007 10:00:00 AM#)FROM t234",
				4);
		checkQuery(
				"select dateDiff('ww',#07/22/2007 11:00:00 AM#,#11/22/2007 12:00:00 AM#)FROM t234",
				17);
		checkQuery(
				"select dateDiff('h',#10/22/2007 08:01:00 AM#,#10/22/2006 04:00:00 AM#)FROM t234",
				-8764);
		checkQuery(
				"select dateDiff('h',#10/22/2007 10:07:00 AM#,#10/22/2007 11:07:00 AM#)FROM t234",
				1);
		checkQuery(
				"select dateDiff('h',#10/22/2007 11:00:00 AM#,#10/22/2007 10:07:00 AM#)FROM t234",
				-1);

		checkQuery(
				"select dateDiff('n',#10/22/2007 08:00:00 AM#,#10/22/2003 04:00:00 AM#)FROM t234",
				-2104080);
		checkQuery(
				"select dateDiff('h',#10/22/2007 08:00:00 AM#,#10/22/2005 04:00:00 AM#)FROM t234",
				-17524);
	}

	public void testDatePart() throws SQLException, IOException {
		checkQuery("select datePart('yyyy',#11/22/1992 10:42:58 PM#)FROM t234",
				1992);
		checkQuery("select datePart('q',#11/22/1992 10:42:58 PM#)FROM t234", 4);
		checkQuery("select datePart('d',#11/22/1992 10:42:58 PM#)FROM t234", 22);
		checkQuery("select datePart('y',#11/22/1992 10:42:58 PM#)FROM t234",
				327);
		checkQuery("select datePart('ww',#11/22/1992 10:42:58 PM#)FROM t234",
				48);
		checkQuery("select datePart('ww',#11/22/2006 10:42:58 PM#,3)FROM t234",48);
		checkQuery("select datePart('w',#05/8/2013#,7), datePart('ww',#11/22/2006 10:42:58 PM#,6,3)FROM t234 ",5,46);
		checkQuery("select datePart('w',#05/13/1992 10:42:58 PM#)FROM t234", 4);
		checkQuery("select datePart('h',#05/13/1992 10:42:58 PM#)FROM t234", 22);
		checkQuery("select datePart('n',#05/13/1992 10:42:58 PM#)FROM t234", 42);
		checkQuery("select datePart('s',#05/13/1992 10:42:58 PM#)FROM t234", 58);
		
	}

	public void testDateSerial() throws SQLException, IOException,
			ParseException {
		checkQuery("select dateserial(1998,5, 10)FROM t234", SDF
				.parse("1998-05-10 00:00:00"));
		checkQuery(
				"select 'It works, I can''t believe it.' FROM t234 WHERE #05/13/1992#=dateserial(1992,05,13)",
				"It works, I can't believe it.");
	}

	public void testFormatNumber() throws SQLException, IOException {
		checkQuery("select format(num,'percent') from t234", "-111055,40%");
		checkQuery("select format(num,'fixed')   from t234", "-1110,55");
		checkQuery("select format(num,'standard')   from t234", "-1,110.55");
		checkQuery("select format(num,'general number')   from t234",
				"-1110,554");
		checkQuery("select format(num,'on/off')  from t234", "On");
		checkQuery("select format(num,'true/false') from t234", "True");
		checkQuery("select format(num,'yes/no')  from t234", "Yes");
		checkQuery("select Format (11111210.6, '#,##0.00') from t234",
				"11,111,210.60");
		checkQuery("select Format (1111111210.6, 'Scientific') from t234",
				"1,11E+09");
		checkQuery(
				"select Format (0.00000000000000015661112106, 'Scientific') from t234",
				"1,57E-16");
	}

	public void testFormatDate() throws SQLException, IOException {
		checkQuery(
				"select format(#05/13/1994 10:42:58 PM#,'Long date') from t234",
				"Friday 13 May 1994");
		checkQuery(
				"select format(#05/13/1994 10:42:58 PM#,'Medium date') from t234",
				"13-May-94");
		checkQuery(
				"select format(#05/13/1994 10:42:58 PM#,'Short date') from t234",
				"13/05/1994");
		checkQuery(
				"select format(#05/13/1994 10:42:58 PM#,'Long time') from t234",
				"22:42:58");
		checkQuery(
				"select format(#05/13/1994 10:42:18 PM#,'Medium time') from t234",
				"10:42");
		checkQuery(
				"select format(#05/13/1994 10:42:58 PM#,'Short time') from t234",
				"22:42");
		checkQuery(
				"select format(#05/13/1994 10:42:58 PM#,'General date') from t234",
				"13/05/1994 22:42:58");
	}

	public void testSign() throws SQLException, IOException {
		checkQuery("select sign (0),sign(-20.4),sign(4)from t234", 0, -1, 1);

	}

	public void testWeekDayName() throws SQLException, IOException {

		checkQuery("select weekDayName(3) from t234", "Tuesday");
		checkQuery("select weekDayName(3,true) from t234", "Tue");
		checkQuery("select  WeekdayName (3, TRUE, 2)  from t234", "Wed");
	}

	public void testMonthName() throws SQLException, IOException {

		checkQuery("select MonthName(3) from t234", "March");
		checkQuery("select MonthName(3, true) from t234", "Mar");

	}

	public void testStr() throws SQLException, IOException {

		checkQuery("select str(id),str(num),str(4.5555555) from t234",
				" 1234", "-1110.554", " 4.5555555");

	}

	public void testDateValue() throws SQLException, IOException,
			ParseException {

		checkQuery("select dateValue(#11/22/2003 10:42:58 PM#) from t234", SDF
				.parse("2003-11-22 00:00:00.0"));
		checkQuery("select dateValue('6/30/2004') from t234", SDF
				.parse("2004-06-30 00:00:00.0"));

	}

	public void testFormatString() throws SQLException, IOException,
			ParseException {
		checkQuery("select format('05/13/1994','Long date') from t234",
				"Friday 13 May 1994");
		checkQuery("select format('0.6','percent') from t234", "60,00%");
	}

}
