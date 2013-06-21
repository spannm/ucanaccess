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
package net.ucanaccess.converters;

import java.math.BigDecimal;
import java.math.MathContext;

import java.sql.Timestamp;
import java.text.DateFormatSymbols;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import net.ucanaccess.converters.TypesMap.AccessType;
import net.ucanaccess.ext.FunctionType;
import net.ucanaccess.jdbc.UcanaccessSQLException;
import net.ucanaccess.jdbc.UcanaccessSQLException.ExceptionMessages;

public class Functions {
	private static Double rnd;
	private static Double lastRnd;
	public final static SimpleDateFormat[] SDFA = new SimpleDateFormat[] {
			new SimpleDateFormat("MMM dd,yyyy"),
			new SimpleDateFormat("MM dd,yyyy"),
			new SimpleDateFormat("MM/dd/yyyy"),
			new SimpleDateFormat("MMM dd hh:mm:ss"),
			new SimpleDateFormat("MM dd hh:mm:ss"),
			new SimpleDateFormat("yyyy-MM-dd hh:mm:ss"),
			new SimpleDateFormat("yyyy-MM-dd"),
			new SimpleDateFormat("MM/dd/yyyy hh:mm:ss") };
	public static final SimpleDateFormat SDFBB = new SimpleDateFormat(
			"yyyy-MM-dd");

	@FunctionType(functionName = "ASC", argumentTypes = { AccessType.MEMO }, returnType = AccessType.LONG)
	public static Integer asc(String s) {
		if (s == null || s.length() == 0)
			return null;
		return (int) s.charAt(0);
	}

	@FunctionType(functionName = "ATN", argumentTypes = { AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
	public static double atn(double v) {
		return Math.atan(v);
	}

	@FunctionType(functionName = "CBOOL", argumentTypes = { AccessType.NUMERIC }, returnType = AccessType.YESNO)
	public static boolean cbool(BigDecimal value) {
		return cbool((Object) value);
	}

	@FunctionType(functionName = "CBOOL", argumentTypes = { AccessType.YESNO }, returnType = AccessType.YESNO)
	public static boolean cbool(Boolean value) {
		return cbool((Object) value);
	}

	private static boolean cbool(Object obj) {
		boolean r = (obj instanceof Boolean) ? (Boolean) obj
				: (obj instanceof String) ? Boolean.valueOf((String) obj)
						: (obj instanceof Number) ? ((Number) obj)
								.doubleValue() != 0 : false;
		return r;
	}

	@FunctionType(functionName = "CBOOL", argumentTypes = { AccessType.MEMO }, returnType = AccessType.YESNO)
	public static boolean cbool(String value) {
		return cbool((Object) value);
	}

	@FunctionType(functionName = "CCUR", argumentTypes = { AccessType.CURRENCY }, returnType = AccessType.CURRENCY)
	public static BigDecimal ccur(BigDecimal value)
			throws UcanaccessSQLException {
		return value.setScale(4, BigDecimal.ROUND_HALF_UP);// .doubleValue();
	}

	@FunctionType(functionName = "CDATE", argumentTypes = { AccessType.MEMO }, returnType = AccessType.DATETIME)
	public static Timestamp cdate(String dt) {
		return dateValue(dt);
	}

	@FunctionType(functionName = "CDBL", argumentTypes = { AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
	public static Double cdbl(Double value) throws UcanaccessSQLException {
		return value;
	}

	@FunctionType(functionName = "CDEC", argumentTypes = { AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
	public static Double cdec(Double value) throws UcanaccessSQLException {
		return value;
	}

	@FunctionType(functionName = "CINT", argumentTypes = { AccessType.DOUBLE }, returnType = AccessType.INTEGER)
	public static Short cint(Double value) throws UcanaccessSQLException {
		return new BigDecimal((long) Math.floor(value + 0.499999999999999d))
				.shortValueExact();
	}

	@FunctionType(functionName = "CLONG", argumentTypes = { AccessType.DOUBLE }, returnType = AccessType.LONG)
	public static Integer clong(Double value) throws UcanaccessSQLException {
		return (int) Math.floor(value + 0.499999999999999d);
	}

	@FunctionType(functionName = "CSIGN", argumentTypes = { AccessType.DOUBLE }, returnType = AccessType.SINGLE)
	public static double csign(double value) {
		MathContext mc = new MathContext(7);
		return new BigDecimal(value, mc).doubleValue();
	}

	@FunctionType(functionName = "CSTR", argumentTypes = { AccessType.YESNO }, returnType = AccessType.MEMO)
	public static String cstr(Boolean value) throws UcanaccessSQLException {
		return cstr((Object) value);
	}

	@FunctionType(functionName = "CSTR", argumentTypes = { AccessType.DOUBLE }, returnType = AccessType.MEMO)
	public static String cstr(double value) throws UcanaccessSQLException {
		return cstr((Object) value);
	}

	@FunctionType(functionName = "CSTR", argumentTypes = { AccessType.LONG }, returnType = AccessType.MEMO)
	public static String cstr(int value) throws UcanaccessSQLException {
		return cstr((Object) value);
	}

	public static String cstr(Object value) throws UcanaccessSQLException {
		return value == null ? null : format(value.toString(), "");
	}

	@FunctionType(functionName = "CSTR", argumentTypes = { AccessType.DATETIME }, returnType = AccessType.MEMO)
	public static String cstr(Timestamp value) throws UcanaccessSQLException {
		return value == null ? null : format(value, "general date");
	}

	@FunctionType(functionName = "CVAR", argumentTypes = { AccessType.DOUBLE }, returnType = AccessType.MEMO)
	public static String cvar(Double value) throws UcanaccessSQLException {
		return format(value, "general number");
	}

	@FunctionType(namingConflict = true, functionName = "DATEADD", argumentTypes = {
			AccessType.MEMO, AccessType.LONG, AccessType.DATETIME }, returnType = AccessType.DATETIME)
	public static Date dateAdd(String intv, int vl, Date dt)
			throws UcanaccessSQLException {
		if (dt == null || intv == null)
			return null;
		Calendar cl = Calendar.getInstance();
		cl.setTime(dt);
		if (intv.equalsIgnoreCase("yyyy")) {
			cl.add(Calendar.YEAR, vl);
		} else if (intv.equalsIgnoreCase("q")) {
			cl.add(Calendar.MONTH, vl * 3);
		} else if (intv.equalsIgnoreCase("y") || intv.equalsIgnoreCase("d")) {
			cl.add(Calendar.DAY_OF_YEAR, vl);
		} else if (intv.equalsIgnoreCase("m")) {
			cl.add(Calendar.MONTH, vl);
		} else if (intv.equalsIgnoreCase("w")) {
			cl.add(Calendar.DAY_OF_WEEK, vl);
		} else if (intv.equalsIgnoreCase("ww")) {
			cl.add(Calendar.WEEK_OF_YEAR, vl);
		} else if (intv.equalsIgnoreCase("h")) {
			cl.add(Calendar.HOUR, vl);
		} else if (intv.equalsIgnoreCase("n")) {
			cl.add(Calendar.MINUTE, vl);
		} else if (intv.equalsIgnoreCase("s")) {
			cl.add(Calendar.SECOND, vl);
		} else
			throw new UcanaccessSQLException(
					ExceptionMessages.INVALID_INTERVAL_VALUE);
		return (dt instanceof Timestamp) ? new Timestamp(cl.getTimeInMillis())
				: new java.sql.Date(cl.getTimeInMillis());
	}

	@FunctionType(namingConflict = true, functionName = "DATEADD", argumentTypes = {
			AccessType.MEMO, AccessType.LONG, AccessType.DATETIME }, returnType = AccessType.DATETIME)
	public static Timestamp dateAdd(String intv, int vl, Timestamp dt)
			throws UcanaccessSQLException {
		return (Timestamp) dateAdd(intv, vl, (Date) dt);
	}

	@FunctionType(namingConflict = true, functionName = "DATEDIFF", argumentTypes = {
			AccessType.MEMO, AccessType.DATETIME, AccessType.DATETIME }, returnType = AccessType.LONG)
	public static Integer dateDiff(String intv, Timestamp dt1, Timestamp dt2)
			throws UcanaccessSQLException {
		if (dt1 == null || intv == null || dt2 == null)
			return null;
		Calendar clMin = Calendar.getInstance();
		Calendar clMax = Calendar.getInstance();
		int sign = dt1.after(dt2) ? -1 : 1;
		if (sign == 1) {
			clMax.setTime(dt2);
			clMin.setTime(dt1);
		} else {
			clMax.setTime(dt1);
			clMin.setTime(dt2);
		}
		clMin.set(Calendar.MILLISECOND, 0);
		clMax.set(Calendar.MILLISECOND, 0);
		Integer result;
		if (intv.equalsIgnoreCase("yyyy")) {
			result = clMax.get(Calendar.YEAR) - clMin.get(Calendar.YEAR);
		} else if (intv.equalsIgnoreCase("q")) {
			result = dateDiff("yyyy", dt1, dt2) * 4
					+ (clMax.get(Calendar.MONTH) - clMin.get(Calendar.MONTH))
					/ 3;
		} else if (intv.equalsIgnoreCase("y") || intv.equalsIgnoreCase("d")) {
			result = (int) Math.rint(((double) (clMax.getTimeInMillis() - clMin
					.getTimeInMillis()) / (1000 * 60 * 60 * 24)));
		} else if (intv.equalsIgnoreCase("m")) {
			result = dateDiff("yyyy", dt1, dt2) * 12
					+ (clMax.get(Calendar.MONTH) - clMin.get(Calendar.MONTH));
		} else if (intv.equalsIgnoreCase("w") || intv.equalsIgnoreCase("ww")) {
			result = (int) Math
					.floor(((double) (clMax.getTimeInMillis() - clMin
							.getTimeInMillis()) / (1000 * 60 * 60 * 24 * 7)));
		} else if (intv.equalsIgnoreCase("h")) {
			result = (int) Math
					.round(((double) (clMax.getTime().getTime() - clMin
							.getTime().getTime())) / (1000d * 60 * 60));
		} else if (intv.equalsIgnoreCase("n")) {
			result = (int) Math.rint(((double) (clMax.getTimeInMillis() - clMin
					.getTimeInMillis()) / (1000 * 60)));
		} else if (intv.equalsIgnoreCase("s")) {
			result = (int) Math.rint(((double) (clMax.getTimeInMillis() - clMin
					.getTimeInMillis()) / 1000));
		} else
			throw new UcanaccessSQLException(
					ExceptionMessages.INVALID_INTERVAL_VALUE);
		return result * sign;
	}

	@FunctionType(namingConflict = true, functionName = "DATEPART", argumentTypes = {
			AccessType.MEMO, AccessType.DATETIME, AccessType.LONG }, returnType = AccessType.LONG)
	public static Integer datePart(String intv, Timestamp dt,
			Integer firstDayOfWeek) throws UcanaccessSQLException {
		Integer ret = (intv.equalsIgnoreCase("ww")) ? datePart(intv, dt,
				firstDayOfWeek, 1) : datePart(intv, dt);
		if (intv.equalsIgnoreCase("w") && firstDayOfWeek > 1) {
			Calendar cl = Calendar.getInstance();
			cl.setTime(dt);
			ret = cl.get(Calendar.DAY_OF_WEEK) - firstDayOfWeek + 1;
			if (ret <= 0)
				ret = 7 + ret;
		}
		return ret;
	}

	@FunctionType(namingConflict = true, functionName = "DATEPART", argumentTypes = {
			AccessType.MEMO, AccessType.DATETIME, AccessType.LONG,
			AccessType.LONG }, returnType = AccessType.LONG)
	public static Integer datePart(String intv, Timestamp dt,
			Integer firstDayOfWeek, Integer firstWeekOfYear)
			throws UcanaccessSQLException {
		Integer ret = datePart(intv, dt);
		if (intv.equalsIgnoreCase("ww")
				&& (firstWeekOfYear > 1 || firstDayOfWeek > 1)) {
			Calendar cl = Calendar.getInstance();
			cl.setTime(dt);
			cl.set(Calendar.MONTH, Calendar.JANUARY);
			cl.set(Calendar.DAY_OF_MONTH, 1);
			Calendar cl1 = Calendar.getInstance();
			cl1.setTime(dt);
			if (firstDayOfWeek == 0)
				firstDayOfWeek = 1;
			int dow = cl.get(Calendar.DAY_OF_WEEK) - firstDayOfWeek + 1;
			if (dow <= 0) {
				dow = 7 + dow;
				if (cl1.get(Calendar.DAY_OF_WEEK) - firstDayOfWeek >= 0)
					ret++;
			}
			if (dow > 4 && firstWeekOfYear == 2) {
				ret--;
			}
			if (dow > 1 && firstWeekOfYear == 3) {
				ret--;
			}
		}
		return ret;
	}

	@FunctionType(namingConflict = true, functionName = "DATEPART", argumentTypes = {
			AccessType.MEMO, AccessType.DATETIME }, returnType = AccessType.LONG)
	public static Integer datePart(String intv, Timestamp dt)
			throws UcanaccessSQLException {
		if (dt == null || intv == null)
			return null;
		Calendar cl = Calendar.getInstance(Locale.US);
		cl.setTime(dt);
		if (intv.equalsIgnoreCase("yyyy")) {
			return cl.get(Calendar.YEAR);
		} else if (intv.equalsIgnoreCase("q")) {
			return (int) Math.ceil((cl.get(Calendar.MONTH) + 1) / 3d);
		} else if (intv.equalsIgnoreCase("d")) {
			return cl.get(Calendar.DAY_OF_MONTH);
		} else if (intv.equalsIgnoreCase("y")) {
			return cl.get(Calendar.DAY_OF_YEAR);
		} else if (intv.equalsIgnoreCase("m")) {
			return cl.get(Calendar.MONTH) + 1;
		} else if (intv.equalsIgnoreCase("ww")) {
			return cl.get(Calendar.WEEK_OF_YEAR);
		} else if (intv.equalsIgnoreCase("w")) {
			return cl.get(Calendar.DAY_OF_WEEK);
		} else if (intv.equalsIgnoreCase("h")) {
			return cl.get(Calendar.HOUR_OF_DAY);
		} else if (intv.equalsIgnoreCase("n")) {
			return cl.get(Calendar.MINUTE);
		} else if (intv.equalsIgnoreCase("s")) {
			return cl.get(Calendar.SECOND);
		} else
			throw new UcanaccessSQLException(
					ExceptionMessages.INVALID_INTERVAL_VALUE);
	}

	@FunctionType(functionName = "DATESERIAL", argumentTypes = {
			AccessType.LONG, AccessType.LONG, AccessType.LONG }, returnType = AccessType.DATETIME)
	public static Timestamp dateSerial(int year, int month, int day) {
		Calendar cl = Calendar.getInstance();
		cl.setLenient(true);
		cl.set(Calendar.YEAR, year);
		cl.set(Calendar.MONTH, month - 1);
		cl.set(Calendar.DAY_OF_MONTH, day);
		cl.set(Calendar.HOUR_OF_DAY, 0);
		cl.set(Calendar.MINUTE, 0);
		cl.set(Calendar.SECOND, 0);
		cl.set(Calendar.MILLISECOND, 0);
		return new Timestamp(cl.getTime().getTime());
	}

	@FunctionType(functionName = "DATEVALUE", argumentTypes = { AccessType.MEMO }, returnType = AccessType.DATETIME)
	public static Timestamp dateValue(String dt) {
		for (SimpleDateFormat sdf : SDFA)
			try {
				sdf.setLenient(true);
				return new Timestamp(sdf.parse(dt).getTime());
			} catch (ParseException e) {
			}
		return null;
	}

	@FunctionType(functionName = "DATEVALUE", argumentTypes = { AccessType.DATETIME }, returnType = AccessType.DATETIME)
	public static Timestamp dateValue(Timestamp dt) {
		Calendar cl = Calendar.getInstance();
		cl.setTime(dt);
		cl.set(Calendar.HOUR_OF_DAY, 0);
		cl.set(Calendar.MINUTE, 0);
		cl.set(Calendar.SECOND, 0);
		cl.set(Calendar.MILLISECOND, 0);
		return new Timestamp(cl.getTime().getTime());
	}

	@FunctionType(functionName = "FORMAT", argumentTypes = { AccessType.DOUBLE,
			AccessType.TEXT }, returnType = AccessType.TEXT)
	public static String format(double d, String par)
			throws UcanaccessSQLException {
		if ("percent".equalsIgnoreCase(par)) {
			return (new BigDecimal(d * 100).setScale(2,
					BigDecimal.ROUND_HALF_UP) + "%").replace(".", ",");
		}
		if ("fixed".equalsIgnoreCase(par)) {
			return new BigDecimal(d).setScale(2, BigDecimal.ROUND_HALF_UP)
					.toString().replace(".", ",");
		}
		if ("standard".equalsIgnoreCase(par)) {
			DecimalFormat formatter = new DecimalFormat("###,###.##");
			return formatter.format(d);
		}
		if ("general number".equalsIgnoreCase(par)) {
			DecimalFormat formatter = new DecimalFormat();
			DecimalFormatSymbols dfs = new DecimalFormatSymbols();
			dfs.setDecimalSeparator(',');
			formatter.setGroupingUsed(false);
			formatter.setDecimalFormatSymbols(dfs);
			return formatter.format(d);
		}
		if ("yes/no".equalsIgnoreCase(par)) {
			return d == 0 ? "No" : "Yes";
		}
		if ("true/false".equalsIgnoreCase(par)) {
			return d == 0 ? "False" : "True";
		}
		if ("On/Off".equalsIgnoreCase(par)) {
			return d == 0 ? "Off" : "On";
		}
		if ("Scientific".equalsIgnoreCase(par)) {
			return String.format(Locale.US, "%6.2E", d).replace(".", ",");
		}
		try {
			DecimalFormat formatter = new DecimalFormat(par);
			return formatter.format(d);
		} catch (Exception e) {
			throw new UcanaccessSQLException(e);
		}
	}

	@FunctionType(functionName = "FORMAT", argumentTypes = { AccessType.TEXT,
			AccessType.TEXT }, returnType = AccessType.TEXT)
	public static String format(String s, String par)
			throws UcanaccessSQLException {
		if (isNumeric(s)) {
			return format(Double.parseDouble(s), par);
		}
		if (isDate(s)) {
			return format(dateValue(s), par);
		}
		return s;
	}

	@FunctionType(functionName = "FORMAT", argumentTypes = {
			AccessType.DATETIME, AccessType.TEXT }, returnType = AccessType.TEXT)
	public static String format(Timestamp t, String par)
			throws UcanaccessSQLException {
		if ("long date".equalsIgnoreCase(par)) {
			return new SimpleDateFormat("EEEE d MMMM yyyy").format(t);
		}
		if ("medium date".equalsIgnoreCase(par)) {
			return new SimpleDateFormat("d-MMM-yy").format(t);
		}
		if ("short date".equalsIgnoreCase(par)) {
			return new SimpleDateFormat("dd/MM/yyyy").format(t);
		}
		if ("general date".equalsIgnoreCase(par)) {
			return new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(t);
		}
		if ("long time".equalsIgnoreCase(par)) {
			return new SimpleDateFormat("HH:mm:ss").format(t);
		}
		if ("medium time".equalsIgnoreCase(par)) {
			return new SimpleDateFormat("hh:mm").format(t);
		}
		if ("short time".equalsIgnoreCase(par)) {
			return new SimpleDateFormat("HH:mm").format(t);
		}
		if ("q".equalsIgnoreCase(par)) {
			return String.valueOf(datePart(par, t));
		}
		return new SimpleDateFormat(par.replaceAll("m", "M").replaceAll("n",
				"m")).format(t);
	}

	@FunctionType(functionName = "IIF", argumentTypes = { AccessType.YESNO,
			AccessType.MEMO, AccessType.MEMO }, returnType = AccessType.MEMO)
	public static String iif(boolean b, String o, String o1) {
		return b ? o : o1;
	}

	@FunctionType(functionName = "INSTR", argumentTypes = { AccessType.LONG,
			AccessType.MEMO, AccessType.MEMO }, returnType = AccessType.LONG)
	public static Integer instr(Integer start, String text, String search) {
		return instr(start, text, search, -1);
	}

	@FunctionType(functionName = "INSTR", argumentTypes = { AccessType.LONG,
			AccessType.MEMO, AccessType.MEMO, AccessType.LONG }, returnType = AccessType.LONG)
	public static Integer instr(Integer start, String text, String search,
			Integer compare) {
		start--;
		if (compare != 0)
			text = text.toLowerCase();
		if (text.length() <= start) {
			return 0;
		} else
			text = text.substring(start);
		return text.indexOf(search) + start + 1;
	}

	@FunctionType(functionName = "INSTR", argumentTypes = { AccessType.MEMO,
			AccessType.MEMO }, returnType = AccessType.LONG)
	public static Integer instr(String text, String search) {
		return instr(1, text, search, -1);
	}

	@FunctionType(functionName = "INSTR", argumentTypes = { AccessType.MEMO,
			AccessType.MEMO, AccessType.LONG }, returnType = AccessType.LONG)
	public static Integer instr(String text, String search, Integer compare) {
		return instr(1, text, search, compare);
	}

	@FunctionType(functionName = "INSTRREV", argumentTypes = { AccessType.TEXT,
			AccessType.TEXT }, returnType = AccessType.LONG)
	public static Integer instrrev(String text, String search) {
		return instrrev(text, search, -1, -1);
	}

	@FunctionType(functionName = "INSTRREV", argumentTypes = { AccessType.MEMO,
			AccessType.MEMO, AccessType.LONG }, returnType = AccessType.LONG)
	public static Integer instrrev(String text, String search, Integer start) {
		return instrrev(text, search, start, -1);
	}

	@FunctionType(functionName = "INSTRREV", argumentTypes = { AccessType.MEMO,
			AccessType.MEMO, AccessType.LONG, AccessType.LONG }, returnType = AccessType.LONG)
	public static Integer instrrev(String text, String search, Integer start,
			Integer compare) {
		if (compare != 0)
			text = text.toLowerCase();
		if (text.length() <= start) {
			return 0;
		} else {
			if (start > 0)
				text = text.substring(0, start);
			return text.lastIndexOf(search) + 1;
		}
	}

	@FunctionType(functionName = "ISDATE", argumentTypes = { AccessType.MEMO }, returnType = AccessType.YESNO)
	public static boolean isDate(String dt) {
		return dateValue(dt) != null;
	}

	@FunctionType(functionName = "ISDATE", argumentTypes = { AccessType.DATETIME }, returnType = AccessType.YESNO)
	public static boolean isDate(Timestamp dt) {
		return true;
	}

	@FunctionType(namingConflict = true, functionName = "IsNull", argumentTypes = { AccessType.MEMO }, returnType = AccessType.YESNO)
	public static boolean isNull(String o) {
		return o == null;
	}

	@FunctionType(functionName = "ISNUMERIC", argumentTypes = { AccessType.NUMERIC }, returnType = AccessType.YESNO)
	public static boolean isNumeric(BigDecimal b) {
		return true;
	}

	@FunctionType(functionName = "ISNUMERIC", argumentTypes = { AccessType.MEMO }, returnType = AccessType.YESNO)
	public static boolean isNumeric(String s) {
		try {
			new BigDecimal(s);
			return true;
		} catch (Exception e) {
		}
		return false;
	}

	@FunctionType(functionName = "LEN", argumentTypes = { AccessType.MEMO }, returnType = AccessType.LONG)
	public static Integer len(String o) {
		if (o == null)
			return null;
		return new Integer(o.length());
	}

	@FunctionType(functionName = "MID", argumentTypes = { AccessType.MEMO,
			AccessType.LONG }, returnType = AccessType.MEMO)
	public static String mid(String value, int start) {
		return mid(value, start, value.length());
	}

	@FunctionType(functionName = "MID", argumentTypes = { AccessType.MEMO,
			AccessType.LONG, AccessType.LONG }, returnType = AccessType.MEMO)
	public static String mid(String value, int start, int length) {
		if (value == null)
			return null;
		int len = start - 1 + length;
		if (start < 1)
			throw new RuntimeException("Invalid function call");
		if (len > value.length()) {
			len = value.length();
		}
		return value.substring(start - 1, len);
	}

	@FunctionType(namingConflict = true, functionName = "MONTHNAME", argumentTypes = { AccessType.LONG }, returnType = AccessType.TEXT)
	public static String monthName(int i) throws UcanaccessSQLException {
		return monthName(i, false);
	}

	@FunctionType(namingConflict = true, functionName = "MONTHNAME", argumentTypes = {
			AccessType.LONG, AccessType.YESNO }, returnType = AccessType.TEXT)
	public static String monthName(int i, boolean abbr)
			throws UcanaccessSQLException {
		i--;
		if (i >= 0 && i <= 11) {
			DateFormatSymbols dfs = new DateFormatSymbols();
			return abbr ? dfs.getShortMonths()[i] : dfs.getMonths()[i];
		}
		throw new UcanaccessSQLException(ExceptionMessages.INVALID_MONTH_NUMBER);
	}

	@FunctionType(functionName = "DATE", argumentTypes = {}, returnType = AccessType.DATETIME)
	public static Timestamp date() {
		Calendar cl = Calendar.getInstance();
		cl.set(Calendar.MILLISECOND, 0);
		cl.set(Calendar.SECOND, 0);
		cl.set(Calendar.MINUTE, 0);
		cl.set(Calendar.HOUR_OF_DAY, 0);
		return new Timestamp(cl.getTime().getTime());
	}

	@FunctionType(namingConflict = true, functionName = "NOW", argumentTypes = {}, returnType = AccessType.DATETIME)
	public static Timestamp now() {
		Calendar cl = Calendar.getInstance();
		cl.set(Calendar.MILLISECOND, 0);
		return new Timestamp(cl.getTime().getTime());
	}

	private static Object nz(Object value, Object outher) {
		return value == null ? outher : value;
	}

	@FunctionType(functionName = "NZ", argumentTypes = { AccessType.MEMO }, returnType = AccessType.MEMO)
	public static String nz(String value) {
		return value == null ? "" : value;
	}

	@FunctionType(functionName = "NZ", argumentTypes = { AccessType.MEMO,
			AccessType.MEMO }, returnType = AccessType.MEMO)
	public static String nz(String value, String outher) {
		return (String) nz((Object) value, (Object) outher);
	}

	@FunctionType(functionName = "SIGN", argumentTypes = { AccessType.DOUBLE }, returnType = AccessType.INTEGER)
	public static short sign(double n) {
		return (short) (n == 0 ? 0 : (n > 0 ? 1 : -1));
	}

	@FunctionType(functionName = "SPACE", argumentTypes = { AccessType.LONG }, returnType = AccessType.MEMO)
	public static String space(Integer nr) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < nr; ++i)
			sb.append(' ');
		return sb.toString();
	}

	@FunctionType(functionName = "STR", argumentTypes = { AccessType.DOUBLE }, returnType = AccessType.TEXT)
	public static String str(double d) {
		String pre = d > 0 ? " " : "";
		return Math.round(d) == d ? pre + Math.round(d) : pre + d;
	}

	@FunctionType(functionName = "TIME", argumentTypes = {}, returnType = AccessType.DATETIME)
	public static Timestamp time() {
		Calendar cl = Calendar.getInstance();
		cl.setTime(now());
		cl.set(1899, 11, 30);
		return new java.sql.Timestamp(cl.getTimeInMillis());
	}

	@FunctionType(functionName = "VAL", argumentTypes = { AccessType.NUMERIC }, returnType = AccessType.DOUBLE)
	public static Double val(BigDecimal val1) {
		return val((Object) val1);
	}

	private static Double val(Object val1) {
		if (val1 == null)
			return null;
		String val = val1.toString().trim();
		int lp = val.lastIndexOf(".");
		char[] ca = val.toCharArray();
		StringBuffer sb = new StringBuffer();
		int minLength = 1;
		for (int i = 0; i < ca.length; i++) {
			char c = ca[i];
			if (((c == '-') || (c == '+')) && (i == 0)) {
				++minLength;
				sb.append(c);
			} else if (c == ' ')
				continue;
			else if (Character.isDigit(c)) {
				sb.append(c);
			} else if (c == '.' && i == lp) {
				sb.append(c);
				if (i == 0 || (i == 1 && minLength == 2)) {
					++minLength;
				}
			} else
				break;
		}
		if (sb.length() < minLength)
			return 0.0d;
		else
			return Double.parseDouble(sb.toString());
	}

	@FunctionType(functionName = "VAL", argumentTypes = { AccessType.MEMO }, returnType = AccessType.DOUBLE)
	public static Double val(String val1) {
		return val((Object) val1);
	}

	@FunctionType(functionName = "WEEKDAYNAME", argumentTypes = { AccessType.LONG }, returnType = AccessType.TEXT)
	public static String weekDayName(int i) {
		return weekDayName(i, false);
	}

	@FunctionType(functionName = "WEEKDAYNAME", argumentTypes = {
			AccessType.LONG, AccessType.YESNO }, returnType = AccessType.TEXT)
	public static String weekDayName(int i, boolean abbr) {
		return weekDayName(i, abbr, 1);
	}

	@FunctionType(functionName = "WEEKDAYNAME", argumentTypes = {
			AccessType.LONG, AccessType.YESNO, AccessType.LONG }, returnType = AccessType.TEXT)
	public static String weekDayName(int i, boolean abbr, int s) {
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.DAY_OF_WEEK, i + s - 1);
		String pattern = abbr ? "%ta" : "%tA";
		return String.format(pattern, cal, cal);
	}

	@FunctionType(functionName = "WEEKDAY", argumentTypes = { AccessType.DATETIME }, returnType = AccessType.LONG)
	public static Integer weekDay(Timestamp dt) throws UcanaccessSQLException {
		return datePart("w", dt);
	}

	@FunctionType(functionName = "WEEKDAY", argumentTypes = {
			AccessType.DATETIME, AccessType.LONG }, returnType = AccessType.LONG)
	public static Integer weekDay(Timestamp dt, Integer firstDayOfWeek)
			throws UcanaccessSQLException {
		return datePart("w", dt, firstDayOfWeek);
	}

	@FunctionType(functionName = "STRING", argumentTypes = { AccessType.LONG,
			AccessType.MEMO }, returnType = AccessType.MEMO)
	public static String string(Integer nr, String str)
			throws UcanaccessSQLException {
		if (str == null)
			return null;
		String ret = "";
		for (int i = 0; i < nr; ++i) {
			ret += str.charAt(0);
		}
		return ret;
	}

	@FunctionType(functionName = "TIMESERIAL", argumentTypes = {
			AccessType.LONG, AccessType.LONG, AccessType.LONG }, returnType = AccessType.DATETIME)
	public static Timestamp timeserial(Integer h, Integer m, Integer s) {
		Calendar cl = Calendar.getInstance();
		cl.setTime(now());
		cl.set(1899, 11, 30, h, m, s);
		return new java.sql.Timestamp(cl.getTimeInMillis());
	}

	@FunctionType(functionName = "RND", argumentTypes = {}, returnType = AccessType.DOUBLE)
	public static Double rnd() {
		return rnd(null);
	}

	@FunctionType(functionName = "RND", argumentTypes = { AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
	public static Double rnd(Double d) {
		if (d == null)
			return lastRnd = Math.random();
		if (d > 0)
			return lastRnd = Math.random();
		if (d < 0)
			return rnd == null ? rnd = d : rnd;
		if (d == 0)
			return lastRnd == null ? lastRnd = Math.random() : lastRnd;
		return null;
	}

	@FunctionType(functionName = "NZ", argumentTypes = { AccessType.NUMERIC,
			AccessType.NUMERIC }, returnType = AccessType.NUMERIC)
	public static BigDecimal nz(BigDecimal value, BigDecimal outher) {
		return (BigDecimal) nz((Object) value, (Object) outher);
	}

	@FunctionType(functionName = "NZ", argumentTypes = { AccessType.DOUBLE,
			AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
	public static Double nz(Double value, Double outher) {
		return (Double) nz((Object) value, (Object) outher);
	}
	
	@FunctionType(functionName = "NZ", argumentTypes = { AccessType.LONG,
			AccessType.LONG }, returnType = AccessType.LONG)
	public static Integer nz(Integer value, Integer outher) {
		return (Integer) nz((Object) value, (Object) outher);
	}
	
	
	@FunctionType(functionName = "STRREVERSE", argumentTypes = { AccessType.MEMO}, returnType = AccessType.MEMO)
	public static String strReverse(String value) {
		if(value==null)return null;
		return new StringBuffer(value).reverse().toString();
	}
	
	@FunctionType(functionName = "STRCONV", argumentTypes = { AccessType.MEMO,AccessType.LONG}, returnType = AccessType.MEMO)
	public static String strConv(String value,int ul) {
		if(value==null)return null;
		if(ul==1)value= value.toUpperCase();
		if(ul==2)value= value.toLowerCase();
		return value;
	}
	
}
