package net.ucanaccess.converters;

import static net.ucanaccess.converters.RegionalSettings.getRegionalSettings;

import io.github.spannm.jackcess.DataType;
import net.ucanaccess.converters.TypesMap.AccessType;
import net.ucanaccess.exception.InvalidFunctionParameterException;
import net.ucanaccess.exception.InvalidIntervalValueException;
import net.ucanaccess.exception.UcanaccessRuntimeException;
import net.ucanaccess.exception.UcanaccessSQLException;
import net.ucanaccess.ext.FunctionType;
import net.ucanaccess.util.Try;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.security.SecureRandom;
import java.sql.Timestamp;
import java.text.*;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.TextStyle;
import java.time.temporal.ChronoUnit;
import java.time.temporal.WeekFields;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Functions {
    private static final Logger LOGGER = System.getLogger(Functions.class.getName());
    private static SecureRandom random;
    private static Double       rnd;
    private static Double       lastRnd;
    private static final double APPROX = 0.00000001;

    private static final String GENERAL_DATE = "general date";

    /** Day zero of Access date values. */
    private static final LocalDateTime ACCESS_BASE_DATE         = LocalDateTime.of(1899, 12, 30, 0, 0);
    /** Weeks as in Access by default: starting on Sunday, week 1 contains January 1st. */
    private static final WeekFields    WEEK_FIELDS_SUNDAY_START = WeekFields.of(DayOfWeek.SUNDAY, 1);

    /** Hexadecimal ({@code &H}) or octal ({@code &O} or just {@code &}) number at the start of a string, as in Val. */
    private static final Pattern VAL_RADIX   = Pattern.compile("&(?:[Hh]([0-9A-Fa-f]*)|[Oo]?([0-7]*))");
    /** Decimal number with optional exponent at the start of a string, as in Val. */
    private static final Pattern VAL_DECIMAL = Pattern.compile("([+-]?\\d*(?:\\.\\d*)?)([eEdD][+-]?\\d+)?");

    private Functions() {
    }

    static SimpleDateFormat createSimpleDateFormat(String pt) {
        SimpleDateFormat sdf = new SimpleDateFormat(pt);
        ((GregorianCalendar) sdf.getCalendar()).setGregorianChange(new Date(Long.MIN_VALUE));
        return sdf;
    }

    /**
     * Returns an Integer representing the character code corresponding to the first letter in a string.
     *
     * @param s any valid string expression
     * @return character code
     */
    @FunctionType(functionName = "Asc", argumentTypes = {AccessType.MEMO}, returnType = AccessType.LONG)
    public static Integer asc(String s) {
        return s == null || s.isEmpty() ? null : (int) s.charAt(0);
    }

    @FunctionType(functionName = "Equals", argumentTypes = {AccessType.COMPLEX, AccessType.COMPLEX}, returnType = AccessType.YESNO)
    public static Boolean equals(Object obj1, Object obj2) {
        if (obj1 == null || obj2 == null || !obj1.getClass().equals(obj2.getClass())) {
            return false;
        }
        if (obj1.getClass().isArray()) {
            return Arrays.equals((Object[]) obj1, (Object[]) obj2);
        }
        return obj1.equals(obj2);
    }

    @FunctionType(functionName = "EqualsIgnoreOrder", argumentTypes = {AccessType.COMPLEX, AccessType.COMPLEX}, returnType = AccessType.YESNO)
    public static Boolean equalsIgnoreOrder(Object obj1, Object obj2) {
        if (obj1 == null || obj2 == null || !obj1.getClass().equals(obj2.getClass())) {
            return false;
        }
        if (obj1.getClass().isArray()) {
            List<Object> lo1 = Arrays.asList((Object[]) obj1);
            List<Object> lo2 = Arrays.asList((Object[]) obj2);
            return lo1.containsAll(lo2) && lo2.containsAll(lo1);
        }
        return obj1.equals(obj2);
    }

    @FunctionType(functionName = "Contains", argumentTypes = {AccessType.COMPLEX, AccessType.COMPLEX}, returnType = AccessType.YESNO)
    public static Boolean contains(Object obj1, Object obj2) {
        if (obj1 == null || obj2 == null || !obj1.getClass().isArray()) {
            return false;
        }
        List<Object> arr1 = Arrays.asList((Object[]) obj1);
        List<Object> arr2 = obj2.getClass().isArray() ? Arrays.asList((Object[]) obj2) : Arrays.asList(obj2);
        return arr1.containsAll(arr2);
    }

    /**
     * Returns a double specifying the arctangent of a number.
     *
     * @param number a double or any valid numeric expression.
     * @return arctangent
     */
    @FunctionType(functionName = "Atn", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double atn(double number) {
        return Math.atan(number);
    }

    /**
     * Returns a double specifying the square root of a number.
     *
     * @param number a double greater than or equal to zero
     * @return square root
     */
    @FunctionType(functionName = "Sqr", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double sqr(double number) {
        return Math.sqrt(number);
    }

    @FunctionType(functionName = "CBool", argumentTypes = {AccessType.NUMERIC}, returnType = AccessType.YESNO)
    public static boolean cbool(BigDecimal value) {
        return cboolImpl(value);
    }

    /**
     * Converts a value to a boolean.
     *
     * @param value boolean input
     * @return boolean
     */
    @FunctionType(functionName = "CBool", argumentTypes = {AccessType.YESNO}, returnType = AccessType.YESNO)
    public static boolean cbool(Boolean value) {
        return cboolImpl(value);
    }

    @FunctionType(functionName = "CBool", argumentTypes = {AccessType.MEMO}, returnType = AccessType.YESNO)
    public static boolean cbool(String value) {
        return cboolImpl(value);
    }

    private static boolean cboolImpl(Object obj) {
        if (obj == null) {
            return false;
        } else if (obj instanceof Boolean) {
            return (Boolean) obj;
        } else if (obj instanceof String) {
            return cboolImpl((String) obj);
        } else {
            return obj instanceof Number && ((Number) obj).intValue() != 0;
        }
    }

    /**
     * Converts a string to a boolean like Access: "True" and "False" in any case, otherwise a number, which is true
     * unless it is zero.
     */
    private static boolean cboolImpl(String value) {
        String s = value.trim();
        if ("true".equalsIgnoreCase(s)) {
            return true;
        } else if ("false".equalsIgnoreCase(s)) {
            return false;
        }
        try {
            return new BigDecimal(s).signum() != 0;
        } catch (NumberFormatException ex) {
            throw new UcanaccessRuntimeException(String.format("Type mismatch in function CBool: %s", value), ex);
        }
    }

    /**
     * Converts an expression into a currency value with four decimal places, rounding halves to the even digit like
     * Access.
     */
    @FunctionType(functionName = "CCur", argumentTypes = {AccessType.CURRENCY}, returnType = AccessType.CURRENCY)
    public static BigDecimal ccur(BigDecimal value) {
        return value.setScale(4, RoundingMode.HALF_EVEN);
    }

    /**
     * Converts an expression into a date value.
     */
    @FunctionType(functionName = "CDate", argumentTypes = {AccessType.MEMO}, returnType = AccessType.DATETIME)
    public static Timestamp cdate(String dt) {
        return dateValue(dt, false);
    }

    /**
     * Converts an Access date value to a date: the whole part counts the days since 1899-12-30, the fraction is the
     * time of day. As in Access, the fraction of a negative value is taken as a positive time.
     */
    @FunctionType(functionName = "CDate", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.DATETIME)
    public static Timestamp cdate(Double value) {
        if (value == null) {
            return null;
        }
        long days = value.longValue();
        long millis = Math.round(Math.abs(value - days) * 86_400_000L);
        return Timestamp.valueOf(ACCESS_BASE_DATE.plusDays(days).plus(millis, ChronoUnit.MILLIS));
    }

    /**
     * Converts an expression to a double.
     */
    @FunctionType(functionName = "CDbl", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static Double cdbl(Double number) {
        return number;
    }

    /**
     * Converts a text to a double, using the decimal and grouping separators of the default locale like Access.
     */
    @FunctionType(functionName = "CDbl", argumentTypes = {AccessType.MEMO}, returnType = AccessType.DOUBLE)
    public static Double cdbl(String value) throws UcanaccessSQLException {
        return parseNumber("CDbl", value);
    }

    /**
     * Parses a whole text as a number in the format of the default locale.
     *
     * @param function name of the calling function, for the error message
     * @param value    text to parse, if {@code null}, {@code null} is returned
     * @return parsed number
     * @throws UcanaccessSQLException if the text is not a number
     */
    private static Double parseNumber(String function, String value) throws UcanaccessSQLException {
        if (value == null) {
            return null;
        }
        String s = value.trim();
        ParsePosition pos = new ParsePosition(0);
        Number number = NumberFormat.getNumberInstance().parse(s, pos);
        if (number == null || pos.getIndex() != s.length()) {
            throw new InvalidFunctionParameterException(function, value);
        }
        return number.doubleValue();
    }

    @FunctionType(functionName = "CDec", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static Double cdec(Double number) {
        return number;
    }

    /**
     * Converts an expression to an integer, rounding halves to the nearest even number like Access.
     */
    @FunctionType(functionName = "CInt", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.INTEGER)
    public static Short cint(Double number) {
        return BigDecimal.valueOf(Math.rint(number)).shortValueExact();
    }

    /**
     * Converts a text to an integer, using the number format of the default locale and rounding halves to even.
     */
    @FunctionType(functionName = "CInt", argumentTypes = {AccessType.MEMO}, returnType = AccessType.INTEGER)
    public static Short cint(String value) throws UcanaccessSQLException {
        Double number = parseNumber("CInt", value);
        return number == null ? null : cint(number);
    }

    @FunctionType(functionName = "CInt", argumentTypes = {AccessType.YESNO}, returnType = AccessType.INTEGER)
    public static Short cint(boolean bool) {
        return (short) (bool ? -1 : 0);
    }

    /**
     * Converts an expression to a long integer, rounding halves to the nearest even number like Access.
     */
    @FunctionType(functionName = "CLng", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.LONG)
    public static Integer clng(Double value) {
        return (int) Math.rint(value);
    }

    @FunctionType(functionName = "CLng", argumentTypes = {AccessType.MEMO}, returnType = AccessType.LONG)
    public static Integer clng(String value) throws UcanaccessSQLException {
        return Try.catching(() -> clng(FormatCache.getNoArgs().parse(value).doubleValue()))
            .orThrow(UcanaccessSQLException::new);
    }

    @FunctionType(functionName = "CLng", argumentTypes = {AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer clng(Integer value) {
        return value;
    }

    @FunctionType(functionName = "CLong", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.LONG)
    public static Integer clong(Double value) {
        return clng(value);
    }

    @FunctionType(functionName = "CLong", argumentTypes = {AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer clong(Integer value) {
        return value;
    }

    @FunctionType(functionName = "CLong", argumentTypes = {AccessType.YESNO}, returnType = AccessType.LONG)
    public static Integer clong(boolean value) {
        return value ? -1 : 0;
    }

    @FunctionType(functionName = "CSign", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.SINGLE)
    public static double csign(double value) {
        MathContext mc = new MathContext(7);
        return new BigDecimal(Double.toString(value), mc).doubleValue();
    }

    /**
     * Converts a value to a string.
     */
    @FunctionType(functionName = "CStr", argumentTypes = {AccessType.YESNO}, returnType = AccessType.MEMO)
    public static String cstr(Boolean value) throws UcanaccessSQLException {
        return cstrImpl(value);
    }

    @FunctionType(functionName = "CStr", argumentTypes = {AccessType.TEXT}, returnType = AccessType.MEMO)
    public static String cstr(String value) {
        return value;
    }

    /**
     * Converts a number to text like Access: up to 15 significant digits, the decimal separator of the default locale
     * and no grouping, so 0.5 becomes "0.5" and 1234567.5 becomes "1234567.5" in an English locale.
     */
    @FunctionType(functionName = "CStr", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.MEMO)
    public static String cstr(Double value) {
        if (value == null) {
            return null;
        } else if (!Double.isFinite(value)) {
            return value.toString();
        }
        char separator = DecimalFormatSymbols.getInstance().getDecimalSeparator();
        return plainDigits(value).replace('.', separator);
    }

    @FunctionType(functionName = "CStr", argumentTypes = {AccessType.LONG}, returnType = AccessType.MEMO)
    public static String cstr(Integer value) {
        return value == null ? null : value.toString();
    }

    /**
     * Returns a number rounded to 15 significant digits as plain text with a point as decimal separator.
     */
    private static String plainDigits(double value) {
        return BigDecimal.valueOf(value).round(new MathContext(15)).stripTrailingZeros().toPlainString();
    }

    @FunctionType(functionName = "CStr", argumentTypes = {AccessType.DATETIME}, returnType = AccessType.MEMO)
    public static String cstr(Timestamp value) throws UcanaccessSQLException {
        return value == null ? null : format(value, GENERAL_DATE);
    }

    private static String cstrImpl(Object value) throws UcanaccessSQLException {
        return value == null ? null : format(value.toString(), "", true);
    }

    @FunctionType(functionName = "CVar", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.MEMO)
    public static String cvar(Double value) throws UcanaccessSQLException {
        return format(value, "general number");
    }

    /**
     * Returns a date containing a date to which a specified time interval has been added.
     *
     * @param intv interval of time
     * @param vl number of intervals to add (to get dates in the future) or dedcut (to get dates in the past)
     * @param dt date to which the interval is added
     * @return calculated date
     * @throws UcanaccessSQLException on invalid date interval input
     */
    @FunctionType(namingConflict = true, functionName = "DateAdd",
        argumentTypes = {AccessType.MEMO, AccessType.LONG, AccessType.DATETIME}, returnType = AccessType.DATETIME)
    public static Date dateAdd(String intv, int vl, Date dt) throws UcanaccessSQLException {
        if (dt == null || intv == null) {
            return null;
        }
        LocalDateTime ldt = toLocalDateTime(dt);
        if ("yyyy".equalsIgnoreCase(intv)) {
            ldt = ldt.plusYears(vl);
        } else if ("q".equalsIgnoreCase(intv)) { // quarter
            ldt = ldt.plusMonths(vl * 3L);
        } else if ("m".equalsIgnoreCase(intv)) { // month
            ldt = ldt.plusMonths(vl);
        } else if ("y".equalsIgnoreCase(intv) || "d".equalsIgnoreCase(intv) || "w".equalsIgnoreCase(intv)) { // day of year, day, weekday
            ldt = ldt.plusDays(vl);
        } else if ("ww".equalsIgnoreCase(intv)) { // week
            ldt = ldt.plusWeeks(vl);
        } else if ("h".equalsIgnoreCase(intv)) { // hour
            ldt = ldt.plusHours(vl);
        } else if ("n".equalsIgnoreCase(intv)) { // minute
            ldt = ldt.plusMinutes(vl);
        } else if ("s".equalsIgnoreCase(intv)) { // second
            ldt = ldt.plusSeconds(vl);
        } else {
            throw new InvalidIntervalValueException(intv);
        }
        Timestamp result = Timestamp.valueOf(ldt);
        return dt instanceof Timestamp ? result : new java.sql.Date(result.getTime());
    }

    /**
     * Converts a date to local date and time. Access stores dates without time zone,
     * so date arithmetic must not be affected by daylight saving time changes.
     */
    private static LocalDateTime toLocalDateTime(Date dt) {
        return dt instanceof Timestamp ? ((Timestamp) dt).toLocalDateTime() : new Timestamp(dt.getTime()).toLocalDateTime();
    }

    private static int dayOfWeekSundayFirst(LocalDate date) {
        return date.getDayOfWeek().getValue() % 7 + 1;
    }

    @FunctionType(namingConflict = true, functionName = "DateAdd", argumentTypes = {AccessType.MEMO, AccessType.LONG, AccessType.DATETIME}, returnType = AccessType.DATETIME)
    public static Timestamp dateAdd(String intv, int vl, Timestamp dt) throws UcanaccessSQLException {
        return (Timestamp) dateAdd(intv, vl, (Date) dt);
    }

    @FunctionType(namingConflict = true, functionName = "DateAdd", argumentTypes = {AccessType.MEMO, AccessType.LONG, AccessType.MEMO}, returnType = AccessType.DATETIME)
    public static Timestamp dateAdd(String intv, int vl, String dt) throws UcanaccessSQLException {
        return dt == null ? null : (Timestamp) dateAdd(intv, vl, (Date) dateValue(dt, false));
    }

    @FunctionType(namingConflict = true, functionName = "DateDiff", argumentTypes = {AccessType.MEMO, AccessType.MEMO, AccessType.MEMO}, returnType = AccessType.LONG)
    public static Integer dateDiff(String intv, String dt1, String dt2) throws UcanaccessSQLException {
        return dateDiff(intv, dateValue(dt1, false), dateValue(dt2, false));
    }

    @FunctionType(namingConflict = true, functionName = "DateDiff", argumentTypes = {AccessType.MEMO, AccessType.MEMO, AccessType.DATETIME}, returnType = AccessType.LONG)
    public static Integer dateDiff(String intv, String dt1, Timestamp dt2) throws UcanaccessSQLException {
        return dateDiff(intv, dateValue(dt1, false), dt2);
    }

    @FunctionType(namingConflict = true, functionName = "DateDiff", argumentTypes = {AccessType.MEMO, AccessType.DATETIME, AccessType.MEMO}, returnType = AccessType.LONG)
    public static Integer dateDiff(String intv, Timestamp dt1, String dt2) throws UcanaccessSQLException {
        return dateDiff(intv, dt1, dateValue(dt2, false));
    }

    @FunctionType(namingConflict = true, functionName = "DateDiff", argumentTypes = {AccessType.MEMO, AccessType.DATETIME, AccessType.DATETIME}, returnType = AccessType.LONG)
    @SuppressWarnings("java:S8700") // Access dates have no time zone, so the difference is measured in wall-clock time
    public static Integer dateDiff(String intv, Timestamp dt1, Timestamp dt2) throws UcanaccessSQLException {
        if (dt1 == null || intv == null || dt2 == null) {
            return null;
        }
        int sign = dt1.after(dt2) ? -1 : 1;
        LocalDateTime min = (sign == 1 ? dt1 : dt2).toLocalDateTime().truncatedTo(ChronoUnit.SECONDS);
        LocalDateTime max = (sign == 1 ? dt2 : dt1).toLocalDateTime().truncatedTo(ChronoUnit.SECONDS);
        int years = max.getYear() - min.getYear();
        long millis = Duration.between(min, max).toMillis();
        Integer result;
        if ("yyyy".equalsIgnoreCase(intv)) {
            result = years;
        } else if ("q".equalsIgnoreCase(intv)) {
            result = years * 4 + (max.getMonthValue() - 1) / 3 - (min.getMonthValue() - 1) / 3;
        } else if ("y".equalsIgnoreCase(intv) || "d".equalsIgnoreCase(intv)) {
            result = (int) Math.rint((double) millis / (1000 * 60 * 60 * 24));
        } else if ("m".equalsIgnoreCase(intv)) {
            result = years * 12 + max.getMonthValue() - min.getMonthValue();
        } else if ("w".equalsIgnoreCase(intv) || "ww".equalsIgnoreCase(intv)) {
            result = (int) Math.floor((double) millis / (1000 * 60 * 60 * 24 * 7));
        } else if ("h".equalsIgnoreCase(intv)) {
            result = (int) Math.round(millis / (1000d * 60 * 60));
        } else if ("n".equalsIgnoreCase(intv)) {
            result = (int) Math.rint((double) millis / (1000 * 60));
        } else if ("s".equalsIgnoreCase(intv)) {
            result = (int) Math.rint((double) millis / 1000);
        } else {
            throw new InvalidIntervalValueException(intv);
        }
        return result * sign;
    }

    @FunctionType(namingConflict = true, functionName = "DatePart", argumentTypes = {AccessType.MEMO, AccessType.MEMO, AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer datePart(String intv, String dt, Integer firstDayOfWeek) throws UcanaccessSQLException {
        return datePart(intv, dateValue(dt, false), firstDayOfWeek);
    }

    /**
     * Returns an integer containing the specified part of a given date.
     *
     * @param interval interval of time you want to return
     * @param date value that you want to evaluate
     * @param firstDayOfWeek constant that specifies the first day of the week
     * @return date part
     * @throws UcanaccessSQLException on invalid date interval input
     */
    @FunctionType(namingConflict = true, functionName = "DatePart", argumentTypes = {AccessType.MEMO, AccessType.DATETIME, AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer datePart(String interval, Timestamp date, Integer firstDayOfWeek) throws UcanaccessSQLException {
        Integer ret = "ww".equalsIgnoreCase(interval)
            ? datePart(interval, date, firstDayOfWeek, 1)
            : datePart(interval, date);
        if ("w".equalsIgnoreCase(interval) && firstDayOfWeek > 1) {
            ret = dayOfWeekSundayFirst(date.toLocalDateTime().toLocalDate()) - firstDayOfWeek + 1;
            if (ret <= 0) {
                ret = 7 + ret;
            }
        }
        return ret;
    }

    @FunctionType(namingConflict = true, functionName = "DatePart", argumentTypes = {AccessType.MEMO, AccessType.MEMO, AccessType.LONG, AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer datePart(String intv, String dt, Integer firstDayOfWeek, Integer firstWeekOfYear)
        throws UcanaccessSQLException {
        return datePart(intv, dateValue(dt, false), firstDayOfWeek, firstWeekOfYear);
    }

    @FunctionType(namingConflict = true, functionName = "DatePart", argumentTypes = {AccessType.MEMO, AccessType.DATETIME, AccessType.LONG, AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer datePart(String intv, Timestamp dt, Integer firstDayOfWeek, Integer firstWeekOfYear) throws UcanaccessSQLException {
        Integer ret = datePart(intv, dt);
        if (ret != null && "ww".equalsIgnoreCase(intv) && (firstWeekOfYear > 1 || firstDayOfWeek > 1)) {
            LocalDate date = dt.toLocalDateTime().toLocalDate();
            if (firstDayOfWeek == 0) {
                firstDayOfWeek = 1;
            }
            int dow = dayOfWeekSundayFirst(date.withDayOfYear(1)) - firstDayOfWeek + 1;
            if (dow <= 0) {
                dow = 7 + dow;
                if (dayOfWeekSundayFirst(date) - firstDayOfWeek >= 0) {
                    ret++;
                }
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

    @FunctionType(namingConflict = true, functionName = "DatePart", argumentTypes = {AccessType.MEMO, AccessType.MEMO}, returnType = AccessType.LONG)
    public static Integer datePart(String intv, String dt) throws UcanaccessSQLException {
        return datePart(intv, dateValue(dt, false));
    }

    @FunctionType(namingConflict = true, functionName = "DatePart", argumentTypes = {AccessType.MEMO, AccessType.DATETIME}, returnType = AccessType.LONG)
    public static Integer datePart(String intv, Timestamp dt) throws UcanaccessSQLException {
        if (intv == null || dt == null) {
            return null;
        }
        LocalDateTime ldt = dt.toLocalDateTime();
        if ("yyyy".equalsIgnoreCase(intv)) {
            return ldt.getYear();
        } else if ("q".equalsIgnoreCase(intv)) {
            return (ldt.getMonthValue() + 2) / 3;
        } else if ("d".equalsIgnoreCase(intv)) {
            return ldt.getDayOfMonth();
        } else if ("y".equalsIgnoreCase(intv)) {
            return ldt.getDayOfYear();
        } else if ("m".equalsIgnoreCase(intv)) {
            return ldt.getMonthValue();
        } else if ("ww".equalsIgnoreCase(intv)) {
            return ldt.get(WEEK_FIELDS_SUNDAY_START.weekOfWeekBasedYear());
        } else if ("w".equalsIgnoreCase(intv)) {
            return dayOfWeekSundayFirst(ldt.toLocalDate());
        } else if ("h".equalsIgnoreCase(intv)) {
            return ldt.getHour();
        } else if ("n".equalsIgnoreCase(intv)) {
            return ldt.getMinute();
        } else if ("s".equalsIgnoreCase(intv)) {
            return ldt.getSecond();
        } else {
            throw new InvalidIntervalValueException(intv);
        }
    }

    /**
     * Returns a date for a specified year, month, and day.
     *
     * @param year number between 100 and 9999, inclusive, or a numeric expression
     * @param month any numeric expression
     * @param day any numeric expression
     * @return date
     */
    @FunctionType(functionName = "DateSerial", argumentTypes = {AccessType.LONG, AccessType.LONG, AccessType.LONG}, returnType = AccessType.DATETIME)
    public static Timestamp dateSerial(int year, int month, int day) {
        // month and day may lie outside their usual range and then roll over, like in Access
        return Timestamp.valueOf(LocalDate.of(year, 1, 1).plusMonths(month - 1L).plusDays(day - 1L).atStartOfDay());
    }

    @FunctionType(functionName = "DateValue", argumentTypes = {AccessType.MEMO}, returnType = AccessType.DATETIME)
    public static Timestamp dateValue(String dt) {
        return dateValue(dt, true);
    }

    @FunctionType(functionName = "Timestamp0", argumentTypes = {AccessType.MEMO}, returnType = AccessType.DATETIME)
    public static Timestamp timestamp0(String dt) {
        GregorianCalendar gc = new GregorianCalendar();
        gc.setGregorianChange(new Date(Long.MIN_VALUE));
        Pattern patDate = Pattern.compile(SQLConverter.DATE_FORMAT + "\\s");
        Pattern patTime = Pattern.compile(SQLConverter.HHMMSS_FORMAT);
        Matcher mtc = patDate.matcher(dt);
        if (mtc.find()) {
            gc.set(Integer.parseInt(mtc.group(1)), Integer.parseInt(mtc.group(2)) - 1, Integer.parseInt(mtc.group(3)));
        } else {
            UcanaccessRuntimeException.throwNow("internal error in parsing timestamp");
        }
        mtc = patTime.matcher(dt);
        if (mtc.find()) {
            gc.set(Calendar.HOUR_OF_DAY, Integer.parseInt(mtc.group(1)));
            gc.set(Calendar.MINUTE, Integer.parseInt(mtc.group(2)));
            gc.set(Calendar.SECOND, Integer.parseInt(mtc.group(3)));
        } else {
            UcanaccessRuntimeException.throwNow("internal error in parsing timestamp");
        }
        gc.set(Calendar.MILLISECOND, 0);
        return new Timestamp(gc.getTime().getTime());
    }

    private static Timestamp dateValue(String dt, boolean onlyDate) {
        RegionalSettings reg = getRegionalSettings();
        if (!"true".equalsIgnoreCase(reg.getRS())
            && (!"PM".equalsIgnoreCase(reg.getPM()) || !"AM".equalsIgnoreCase(reg.getAM()))) {
            dt = dt.replaceAll("(?i)" + Pattern.quote(reg.getPM()), "PM")
                .replaceAll("(?i)" + Pattern.quote(reg.getAM()), "AM");
        }

        for (Entry<SimpleDateFormat, Boolean> entry : reg.getDateFormats().entrySet()) {
            SimpleDateFormat sdf = entry.getKey();
            boolean yearOverride = entry.getValue();

            try {
                Timestamp t = new Timestamp(sdf.parse(dt).getTime());
                if (onlyDate) {
                    t = dateValue(t);
                }
                if (yearOverride) {
                    Calendar cl = Calendar.getInstance();
                    int y = cl.get(Calendar.YEAR);
                    cl.setTime(t);
                    cl.set(Calendar.YEAR, y);
                    t = new Timestamp(cl.getTime().getTime());
                }
                return t;
            } catch (ParseException ignored) {
                LOGGER.log(Level.DEBUG, "Ignoring {0}", ignored.toString());
            }
        }
        return null;
    }

    /**
     * Returns a date based on a string. If the given string does not include a year component, this function will use the current year.
     */
    @FunctionType(functionName = "DateValue", argumentTypes = {AccessType.DATETIME}, returnType = AccessType.DATETIME)
    public static Timestamp dateValue(Timestamp dt) {
        return Timestamp.valueOf(dt.toLocalDateTime().toLocalDate().atStartOfDay());
    }

    @FunctionType(functionName = "Format", argumentTypes = {AccessType.DOUBLE, AccessType.TEXT}, returnType = AccessType.TEXT)
    public static String format(Double d, String par) throws UcanaccessSQLException {
        if (d == null) {
            return "";
        } else if ("percent".equalsIgnoreCase(par)) {
            return FormatCache.getZpzz().format(d * 100) + "%";
        } else if ("fixed".equalsIgnoreCase(par)) {
            return FormatCache.getZpzz().format(d);
        } else if ("standard".equalsIgnoreCase(par)) {
            return FormatCache.getSharp().format(d);
        } else if ("general number".equalsIgnoreCase(par)) {
            return FormatCache.getNoGrouping().format(d);
        } else if ("currency".equalsIgnoreCase(par)) {
            return FormatCache.getCurrencyDefault().format(d);
        } else if ("yes/no".equalsIgnoreCase(par)) {
            return d == 0 ? "No" : "Yes";
        } else if ("true/false".equalsIgnoreCase(par)) {
            return d == 0 ? "False" : "True";
        } else if ("On/Off".equalsIgnoreCase(par)) {
            return d == 0 ? "Off" : "On";
        } else if ("Scientific".equalsIgnoreCase(par)) {
            return String.format("%6.2E", d);
        }
        return Try.catching(() -> FormatCache.getDecimalFormat(par).format(d))
            .orThrow(UcanaccessSQLException::new);
    }

    @FunctionType(functionName = "Format", argumentTypes = {AccessType.TEXT, AccessType.TEXT}, returnType = AccessType.TEXT)
    public static String format(String s, String par) throws UcanaccessSQLException {
        if (s == null) {
            return "";
        }
        return format(s, par, false);
    }

    public static String format(String s, String par, boolean incl) throws UcanaccessSQLException {
        if (">".equals(par)) {
            return s.toUpperCase(Locale.getDefault());
        } else if ("<".equals(par)) {
            return s.toLowerCase(Locale.getDefault());
        } else if (isNumeric(s)) {
            if (incl) {
                return format(Double.parseDouble(s), par);
            }
            return Try.catching(() -> format(FormatCache.getNoArgs().parse(s).doubleValue(), par))
                .orThrow(UcanaccessSQLException::new);
        } else if (isDate(s)) {
            return format(dateValue(s, false), par);
        }
        return s;
    }

    private static String formatDate(Timestamp t, String pattern) {
        RegionalSettings reg = getRegionalSettings();
        SimpleDateFormat sdf = createSimpleDateFormat(pattern);
        String ret = sdf.format(t);
        if (!reg.getRS().equalsIgnoreCase("true")) {
            if (!reg.getAM().equals("AM")) {
                ret = ret.replace("AM", reg.getAM());
            }
            if (!reg.getPM().equals("PM")) {
                ret = ret.replace("PM", reg.getPM());
            }
        } else {
            ret = ret.replace(reg.getPM(), "PM");
            ret = ret.replace(reg.getAM(), "AM");
        }
        return ret;

    }

    /**
     * Converts a number to text without a format, like CStr.
     */
    @FunctionType(functionName = "Format", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.TEXT)
    public static String format(Double d) {
        return d == null ? null : cstr(d);
    }

    /**
     * Returns a text unchanged, as Format does without a format.
     */
    @FunctionType(functionName = "Format", argumentTypes = {AccessType.TEXT}, returnType = AccessType.TEXT)
    public static String format(String s) {
        return s;
    }

    /**
     * Converts a date to text without a format, like CStr.
     */
    @FunctionType(functionName = "Format", argumentTypes = {AccessType.DATETIME}, returnType = AccessType.TEXT)
    public static String format(Timestamp t) throws UcanaccessSQLException {
        return cstr(t);
    }

    @FunctionType(functionName = "Format", argumentTypes = {AccessType.DATETIME, AccessType.TEXT}, returnType = AccessType.TEXT)
    public static String format(Timestamp t, String par) throws UcanaccessSQLException {
        if (t == null) {
            return "";
        }
        RegionalSettings reg = getRegionalSettings();

        if ("long date".equalsIgnoreCase(par)) {
            return formatDate(t, reg.getLongDatePattern());
        } else if ("medium date".equalsIgnoreCase(par)) {
            return formatDate(t, reg.getMediumDatePattern());
        } else if ("short date".equalsIgnoreCase(par)) {
            return formatDate(t, reg.getShortDatePattern());
        } else if (GENERAL_DATE.equalsIgnoreCase(par)) {
            return formatDate(t, reg.getGeneralPattern());
        } else if ("long time".equalsIgnoreCase(par)) {
            return formatDate(t, reg.getLongTimePattern());
        } else if ("medium time".equalsIgnoreCase(par)) {
            return formatDate(t, reg.getMediumTimePattern());
        } else if ("short time".equalsIgnoreCase(par)) {
            return formatDate(t, reg.getShortTimePattern());
        } else if ("q".equalsIgnoreCase(par)) {
            return String.valueOf(datePart(par, t));
        }
        return createSimpleDateFormat(par
            .replace('m', 'M')
            .replace('n', 'm')
            .replace("(?i)AM/PM|A/P|AMPM", "a")
            .replace("dddd", "EEEE")).format(t);
    }

    /**
     * Returns one of two parts, depending on the evaluation of an expression.
     */
    @FunctionType(functionName = "IIf", argumentTypes = {AccessType.YESNO, AccessType.MEMO, AccessType.MEMO}, returnType = AccessType.MEMO)
    public static String iif(Boolean b, String o1, String o2) {
        return iifImpl(b, o1, o2);
    }

    @FunctionType(functionName = "IIf", argumentTypes = {AccessType.YESNO, AccessType.LONG, AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer iif(Boolean b, Integer o1, Integer o2) {
        return iifImpl(b, o1, o2);
    }

    @FunctionType(functionName = "IIf", argumentTypes = {AccessType.YESNO, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static Double iif(Boolean b, Double o1, Double o2) {
        return iifImpl(b, o1, o2);
    }

    @FunctionType(functionName = "IIf", argumentTypes = {AccessType.YESNO, AccessType.YESNO, AccessType.YESNO}, returnType = AccessType.YESNO)
    public static Boolean iif(Boolean b, Boolean o1, Boolean o2) {
        return iifImpl(b, o1, o2);
    }

    @FunctionType(functionName = "IIf", argumentTypes = {AccessType.YESNO, AccessType.DATETIME, AccessType.DATETIME}, returnType = AccessType.DATETIME)
    public static Timestamp iif(Boolean b, Timestamp o1, Timestamp o2) {
        return iifImpl(b, o1, o2);
    }

    /**
     * Returns a number or a text, depending on the evaluation of an expression. Access returns a variant here, which
     * UCanAccess represents as text.
     */
    @FunctionType(functionName = "IIf", argumentTypes = {AccessType.YESNO, AccessType.DOUBLE, AccessType.MEMO}, returnType = AccessType.MEMO)
    public static String iif(Boolean b, Double o1, String o2) {
        return iifImpl(b, o1 == null ? null : cstr(o1), o2);
    }

    /**
     * Returns a text or a number, depending on the evaluation of an expression. Access returns a variant here, which
     * UCanAccess represents as text.
     */
    @FunctionType(functionName = "IIf", argumentTypes = {AccessType.YESNO, AccessType.MEMO, AccessType.DOUBLE}, returnType = AccessType.MEMO)
    public static String iif(Boolean b, String o1, Double o2) {
        return iifImpl(b, o1, o2 == null ? null : cstr(o2));
    }

    /**
     * Returns the first or second value, depending on the condition.
     */
    private static <T> T iifImpl(Boolean b, T o1, T o2) {
        return b != null && b ? o1 : o2;
    }

    /**
     * Returns the position of the first occurrence of a string in another string.
     */
    @FunctionType(namingConflict = true, functionName = "InStr", argumentTypes = {AccessType.LONG, AccessType.MEMO, AccessType.MEMO}, returnType = AccessType.LONG)
    public static Integer instr(Integer start, String text, String search) {
        return instr(start, text, search, -1);
    }

    @FunctionType(namingConflict = true, functionName = "InStr", argumentTypes = {AccessType.LONG, AccessType.MEMO, AccessType.MEMO, AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer instr(Integer start, String text, String search, Integer compare) {
        if (start < 1) {
            UcanaccessRuntimeException.throwNow("Invalid function call");
        }
        if (text == null || search == null) {
            return null;
        } else if (text.isEmpty()) {
            return 0;
        } else if (search.isEmpty()) {
            return start;
        } else if (start > text.length()) {
            return 0;
        }
        return indexOf(text, search, start - 1, compare != 0) + 1;
    }

    @FunctionType(namingConflict = true, functionName = "InStr", argumentTypes = {AccessType.MEMO, AccessType.MEMO}, returnType = AccessType.LONG)
    public static Integer instr(String text, String search) {
        return instr(1, text, search, -1);
    }

    @FunctionType(namingConflict = true, functionName = "InStr", argumentTypes = {AccessType.MEMO, AccessType.MEMO, AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer instr(String text, String search, Integer compare) {
        return instr(1, text, search, compare);
    }

    /**
     * Returns the position of the first occurrence of a string in another string, starting from the end of the string.
     */
    @FunctionType(functionName = "InStrRev", argumentTypes = {AccessType.TEXT, AccessType.TEXT}, returnType = AccessType.LONG)
    public static Integer instrrev(String text, String search) {
        return instrrev(text, search, -1, -1);
    }

    @FunctionType(functionName = "InStrRev", argumentTypes = {AccessType.MEMO, AccessType.MEMO, AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer instrrev(String text, String search, Integer start) {
        return instrrev(text, search, start, -1);
    }

    @FunctionType(functionName = "InStrRev", argumentTypes = {AccessType.MEMO, AccessType.MEMO, AccessType.LONG, AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer instrrev(String text, String search, Integer start, Integer compare) {
        if (start == 0 || start < -1) {
            UcanaccessRuntimeException.throwNow("Invalid function call");
        }
        if (text == null || search == null) {
            return null;
        }
        int end = start == -1 ? text.length() : start;
        if (end > text.length()) {
            return 0;
        } else if (search.isEmpty()) {
            return end;
        }
        return lastIndexOf(text, search, end, compare != 0) + 1;
    }

    /**
     * Returns the index of the first occurrence of {@code search} in {@code text} at or after {@code from}, or -1.
     *
     * @param text       the text to search in
     * @param search     the text to search for
     * @param from       the index to start at
     * @param ignoreCase whether to ignore case
     * @return index of the match or -1
     */
    private static int indexOf(String text, String search, int from, boolean ignoreCase) {
        for (int i = from; i <= text.length() - search.length(); i++) {
            if (text.regionMatches(ignoreCase, i, search, 0, search.length())) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Returns the index of the last occurrence of {@code search} in {@code text} that ends at or before {@code end},
     * or -1.
     *
     * @param text       the text to search in
     * @param search     the text to search for
     * @param end        the index the match must end at or before
     * @param ignoreCase whether to ignore case
     * @return index of the match or -1
     */
    private static int lastIndexOf(String text, String search, int end, boolean ignoreCase) {
        for (int i = end - search.length(); i >= 0; i--) {
            if (text.regionMatches(ignoreCase, i, search, 0, search.length())) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Returns {@code true} if the expression is a valid date, otherwise {@code false}.
     */
    @FunctionType(functionName = "IsDate", argumentTypes = {AccessType.MEMO}, returnType = AccessType.YESNO)
    public static boolean isDate(String value) {
        return dateValue(value) != null;
    }

    @FunctionType(functionName = "IsDate", argumentTypes = {AccessType.DATETIME}, returnType = AccessType.YESNO)
    public static boolean isDate(Timestamp value) {
        return true;
    }

    /**
     * Returns {@code true} if the expression is a {@code null} value, otherwise {@code false}.
     */
    @FunctionType(namingConflict = true, functionName = "IsNull", argumentTypes = {AccessType.MEMO}, returnType = AccessType.YESNO)
    public static boolean isNull(String value) {
        return value == null;
    }

    @FunctionType(namingConflict = true, functionName = "IsNull", argumentTypes = {AccessType.DATETIME}, returnType = AccessType.YESNO)
    public static boolean isNull(Timestamp value) {
        return value == null;
    }

    @FunctionType(namingConflict = true, functionName = "IsNull", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.YESNO)
    public static boolean isNull(Double value) {
        return value == null;
    }

    /**
     * Returns {@code true} if the expression is a valid number, otherwise {@code false}.
     */
    @FunctionType(functionName = "IsNumeric", argumentTypes = {AccessType.NUMERIC}, returnType = AccessType.YESNO)
    public static boolean isNumeric(BigDecimal b) {
        return true;
    }

    @FunctionType(functionName = "IsNumeric", argumentTypes = {AccessType.MEMO}, returnType = AccessType.YESNO)
    public static boolean isNumeric(String str) {
        return Try.catching(() -> {
            Currency cr = Currency.getInstance(Locale.getDefault());
            if (str.startsWith(cr.getSymbol())) {
                return isNumeric(str.substring(cr.getSymbol().length()));
            }
            if (str.startsWith("+") || str.startsWith("-")) {
                return isNumeric(str.substring(1));
            }
            DecimalFormatSymbols dfs = DecimalFormatSymbols.getInstance();
            String sep = dfs.getDecimalSeparator() + "";
            String gs = dfs.getGroupingSeparator() + "";
            if (str.startsWith(gs)) {
                return false;
            }
            if (str.startsWith(sep)) {
                return isNumeric(str.substring(1));
            }

            String s;
            if (".".equals(sep)) {
                s = str.replaceAll(gs, "");
            } else {
                s = str.replace(".", "")
                    .replace(sep, ".");
            }
            new BigDecimal(s);
            return true;
        }).orElse(false);
    }

    /**
     * Extracts a substring from a string, starting from the left-most character.
     */
    @FunctionType(functionName = "Left", namingConflict = true, argumentTypes = {AccessType.MEMO, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String left(String input, int i) {
        if (input == null || i < 0) {
            return null;
        } else if (i >= input.length()) {
            return input;
        } else {
            return input.substring(0, i);
        }
    }

    @FunctionType(functionName = "\"LEFT$\"", argumentTypes = {AccessType.MEMO, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String leftS(String input, int i) {
        return left(input, i);
    }

    /**
     * Returns the length of the specified string.
     */
    @FunctionType(functionName = "Len", argumentTypes = {AccessType.MEMO}, returnType = AccessType.LONG)
    public static Integer len(String value) {
        return value == null ? null : value.length();
    }

    /**
     * Returns the length of a number converted to text, like Access does for numeric expressions.
     */
    @FunctionType(functionName = "Len", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.LONG)
    public static Integer len(Double value) {
        return value == null ? null : cstr(value).length();
    }

    /**
     * Extracts a substring from a string (starting at any position).
     */
    @FunctionType(functionName = "Mid", argumentTypes = {AccessType.MEMO, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String mid(String value, int start) {
        return value == null ? null : mid(value, start, value.length());
    }

    @FunctionType(functionName = "Mid", argumentTypes = {AccessType.MEMO, AccessType.LONG, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String mid(String value, int start, int length) {
        if (value == null) {
            return null;
        }
        int len = start - 1 + length;
        if (start < 1) {
            UcanaccessRuntimeException.throwNow("Invalid function call");
        }
        if (len > value.length()) {
            len = value.length();
        }
        return value.substring(start - 1, len);
    }

    /**
     * Returns a string representing the month given a number from 1 to 12.
     */
    @FunctionType(namingConflict = true, functionName = "MonthName", argumentTypes = {AccessType.LONG}, returnType = AccessType.TEXT)
    public static String monthName(int number) throws UcanaccessSQLException {
        return monthName(number, false);
    }

    @FunctionType(namingConflict = true, functionName = "MonthName", argumentTypes = {AccessType.LONG, AccessType.YESNO}, returnType = AccessType.TEXT)
    public static String monthName(int number, boolean abbreviate) throws UcanaccessSQLException {
        if (number >= 1 && number <= 12) {
            DateFormatSymbols dfs = new DateFormatSymbols();
            return abbreviate ? dfs.getShortMonths()[number - 1] : dfs.getMonths()[number - 1];
        }
        throw new UcanaccessSQLException("Invalid month number");
    }

    /**
     * Returns the current system date.
     */
    @FunctionType(functionName = "Date", argumentTypes = {}, returnType = AccessType.DATETIME)
    public static Timestamp date() {
        return Timestamp.valueOf(LocalDate.now(ZoneId.systemDefault()).atStartOfDay());
    }

    /**
     * Returns the current system date and time.
     */
    @FunctionType(namingConflict = true, functionName = "Now", argumentTypes = {}, returnType = AccessType.DATETIME)
    public static Timestamp now() {
        return new Timestamp(System.currentTimeMillis() / 1000 * 1000);
    }

    /**
     * Returns the second argument if the first argument is {@code null}.
     */
    @FunctionType(functionName = "Nz", argumentTypes = {AccessType.MEMO}, returnType = AccessType.MEMO)
    public static String nz(String value) {
        return value == null ? "" : value;
    }

    @FunctionType(functionName = "Nz", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static Double nz(Double value) {
        return value == null ? 0 : value;
    }

    @FunctionType(functionName = "Nz", argumentTypes = {AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer nz(Integer value) {
        return value == null ? 0 : value;
    }

    @FunctionType(functionName = "Nz", argumentTypes = {AccessType.NUMERIC}, returnType = AccessType.NUMERIC)
    public static BigDecimal nz(BigDecimal value) {
        return value == null ? BigDecimal.ZERO : value;
    }

    @FunctionType(functionName = "Nz", argumentTypes = {AccessType.MEMO, AccessType.MEMO}, returnType = AccessType.MEMO)
    public static String nz(String value, String other) {
        return (String) nz(value, (Object) other);
    }

    @FunctionType(functionName = "Nz", argumentTypes = {AccessType.NUMERIC, AccessType.NUMERIC}, returnType = AccessType.NUMERIC)
    public static BigDecimal nz(BigDecimal value, BigDecimal other) {
        return (BigDecimal) nz(value, (Object) other);
    }

    @FunctionType(functionName = "Nz", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static Double nz(Double value, Double other) {
        return (Double) nz(value, (Object) other);
    }

    @FunctionType(functionName = "Nz", argumentTypes = {AccessType.LONG, AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer nz(Integer value, Integer other) {
        return (Integer) nz(value, (Object) other);
    }

    @FunctionType(functionName = "Nz", argumentTypes = {AccessType.DATETIME, AccessType.DATETIME}, returnType = AccessType.DATETIME)
    public static Timestamp nz(Timestamp value, Timestamp other) {
        return (Timestamp) nz(value, (Object) other);
    }

    /**
     * Returns the second argument if the first argument is {@code null}.
     */
    private static Object nz(Object value, Object other) {
        return value == null ? other : value;
    }

    /**
     * Returns the sign of a number. If number &gt; 0, it returns 1. If number = 0, it returns 0.
     */
    @FunctionType(functionName = "Sgn", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.INTEGER)
    public static Short sgn(Double n) {
        return n == null ? null : (short) Math.signum(n);
    }

    @FunctionType(functionName = "Sign", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.INTEGER)
    public static Short sign(Double n) {
        return sgn(n);
    }

    /**
     * Returns a string with a specified number of spaces.
     */
    @FunctionType(functionName = "Space", argumentTypes = {AccessType.LONG}, returnType = AccessType.MEMO)
    public static String space(Integer nr) {
        return " ".repeat(Math.max(0, Objects.requireNonNullElse(nr, 0)));
    }

    /**
     * Returns a string representation of a number. Like in Access, a positive number or zero has a leading space for
     * the sign and the zero before the decimal point is left out, so 0.5 becomes " .5".
     */
    @FunctionType(functionName = "Str", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.TEXT)
    public static String str(Double d) {
        if (d == null) {
            return null;
        }
        String digits = plainDigits(Math.abs(d));
        if (digits.startsWith("0.")) {
            digits = digits.substring(1);
        }
        return (d < 0 ? "-" : " ") + digits;
    }

    /**
     * Returns the current system time.
     */
    @FunctionType(functionName = "Time", argumentTypes = {}, returnType = AccessType.DATETIME)
    public static Timestamp time() {
        return new Timestamp(LocalDateTime.now(ZoneId.systemDefault())
            .withYear(1899).withMonth(12).withDayOfMonth(30)
            .truncatedTo(ChronoUnit.SECONDS)
            .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
    }

    /**
     * Accepts a string as input and returns the numbers found in that string.
     */
    @FunctionType(functionName = "Val", argumentTypes = {AccessType.MEMO}, returnType = AccessType.DOUBLE)
    public static Double val(String val1) {
        return val((Object) val1);
    }

    @FunctionType(functionName = "Val", argumentTypes = {AccessType.NUMERIC}, returnType = AccessType.DOUBLE)
    public static Double val(BigDecimal val1) {
        return val((Object) val1);
    }

    private static Double val(Object val1) {
        if (val1 == null) {
            return null;
        }
        // like Access, ignore blanks, tabs and line feeds anywhere in the string
        String val = val1.toString().replaceAll("[ \\t\\n\\r]", "");
        Matcher radix = VAL_RADIX.matcher(val);
        if (radix.lookingAt()) {
            boolean hex = radix.group(1) != null;
            String digits = hex ? radix.group(1) : radix.group(2);
            if (digits.isEmpty()) {
                return 0.0d;
            }
            long l = Long.parseLong(digits, hex ? 16 : 8);
            // like VBA literals, &HFFFF is an integer (-1) and &HFFFFFFFF a long integer (-1)
            return l <= 0xFFFFL ? (double) (short) l : l <= 0xFFFFFFFFL ? (double) (int) l : (double) l;
        }
        Matcher decimal = VAL_DECIMAL.matcher(val);
        if (!decimal.lookingAt() || !decimal.group(1).matches(".*\\d.*")) {
            return 0.0d;
        }
        String number = decimal.group(1) + Objects.toString(decimal.group(2), "").replaceFirst("[dD]", "e");
        return Double.parseDouble(number);
    }

    /**
     * Returns a string representing the day of the week given a number from 1 to 7.
     */
    @FunctionType(functionName = "WeekdayName", argumentTypes = {AccessType.LONG}, returnType = AccessType.TEXT)
    public static String weekdayName(int number) {
        return weekdayName(number, false);
    }

    @FunctionType(functionName = "WeekdayName", argumentTypes = {AccessType.LONG, AccessType.YESNO}, returnType = AccessType.TEXT)
    public static String weekdayName(int number, boolean abbreviate) {
        int vbSunday = 1;
        return weekdayName(number, abbreviate, vbSunday);
    }

    @FunctionType(functionName = "WeekdayName", argumentTypes = {AccessType.LONG, AccessType.YESNO, AccessType.LONG}, returnType = AccessType.TEXT)
    public static String weekdayName(int num, boolean abbreviate, int firstDayOfWeek) {
        // DayOfWeek starts with Monday, WeekdayName with Sunday (default)
        if (num < 1 || num > 7) {
            UcanaccessRuntimeException.throwNow(String.format("Invalid weekday number for function WeekdayName: %d", num));
        }
        int fdow = Math.min(Math.max(1, firstDayOfWeek), 7);
        int offset = fdow == 1 ? -1 : fdow - 2;
        return DayOfWeek.of(num).plus(offset)
            .getDisplayName(abbreviate ? TextStyle.SHORT : TextStyle.FULL, Locale.getDefault());
    }

    /**
     * Returns a number representing the day of the week (a number from 1 to 7) given a date value.
     */
    @FunctionType(functionName = "WeekDay", argumentTypes = {AccessType.DATETIME}, returnType = AccessType.LONG)
    public static Integer weekDay(Timestamp date) throws UcanaccessSQLException {
        return datePart("w", date);
    }

    @FunctionType(functionName = "WeekDay", argumentTypes = {AccessType.DATETIME, AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer weekDay(Timestamp date, Integer firstDayOfWeek) throws UcanaccessSQLException {
        return datePart("w", date, firstDayOfWeek);
    }

    /**
     * Returns a Variant (String) containing a repeating character string of the length specified.
     */
    @FunctionType(functionName = "String", argumentTypes = {AccessType.LONG, AccessType.MEMO}, returnType = AccessType.MEMO)
    public static String string(Integer nr, String str) {
        if (str == null) {
            return null;
        } else if (str.isEmpty()) {
            return "";
        }

        return str.substring(0, 1).repeat(nr);
    }

    /**
     * Returns a time given an hour, minute, and second value. Like in Access, values out of range carry over into the
     * next unit, so 25 hours are one day and one hour after the base date 1899-12-30. A negative total keeps its days
     * before the base date, while the time of day is the absolute remainder, as Access stores negative dates.
     */
    @FunctionType(functionName = "TimeSerial", argumentTypes = {AccessType.LONG, AccessType.LONG, AccessType.LONG}, returnType = AccessType.DATETIME)
    public static Timestamp timeSerial(Integer h, Integer m, Integer s) {
        if (h == null || m == null || s == null) {
            return null;
        }
        long seconds = h * 3600L + m * 60L + s;
        return Timestamp.valueOf(ACCESS_BASE_DATE
            .plusDays(seconds / 86400)
            .plusSeconds(Math.abs(seconds % 86400)));
    }

    /**
     *  Generates a random number (integer value).
     */
    @FunctionType(functionName = "Rnd", argumentTypes = {}, returnType = AccessType.DOUBLE)
    public static Double rnd() {
        return rnd(null);
    }

    @FunctionType(functionName = "Rnd", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static Double rnd(Double d) {
        if (random == null) {
            random = new SecureRandom();
        }
        if (d == null || d > 0) {
            lastRnd = random.nextDouble();
            return lastRnd;
        }
        if (d < 0) {
            if (rnd == null) {
                rnd = d;
            }
            return rnd;
        }
        if (d == 0) {
            if (lastRnd == null) {
                lastRnd = random.nextDouble();
            }
            return lastRnd;
        }
        return null;
    }

    /**
     * Returns a string whose characters are in reverse order.
     */
    @FunctionType(functionName = "StrReverse", argumentTypes = {AccessType.MEMO}, returnType = AccessType.MEMO)
    public static String strReverse(String value) {
        return value == null ? null : new StringBuilder(value).reverse().toString();
    }

    /**
     * Returns the hexadecimal representation of a number, rounded to a whole number first. Like in Access, negative
     * numbers in the range of a long integer are shown as 32-bit two's complement.
     *
     * @param number number to convert, if {@code null}, {@code null} is returned
     * @return upper case hexadecimal digits
     */
    @FunctionType(namingConflict = true, functionName = "Hex", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.MEMO)
    public static String hex(Double number) {
        if (number == null) {
            return null;
        }
        long l = (long) Math.rint(number);
        String hex = l == (int) l ? Integer.toHexString((int) l) : Long.toHexString(l);
        return hex.toUpperCase(Locale.ROOT);
    }

    /**
     * Replaces all occurrences of a substring, ignoring case like Access queries do by default.
     */
    @FunctionType(namingConflict = true, functionName = "Replace", argumentTypes = {AccessType.MEMO, AccessType.MEMO, AccessType.MEMO}, returnType = AccessType.MEMO)
    public static String replace(String expression, String find, String replacement) throws UcanaccessSQLException {
        return replace(expression, find, replacement, 1, -1, -1);
    }

    /**
     * Replaces all occurrences of a substring, returning the string from the start position on.
     */
    @FunctionType(namingConflict = true, functionName = "Replace", argumentTypes = {AccessType.MEMO, AccessType.MEMO, AccessType.MEMO, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String replace(String expression, String find, String replacement, Integer start) throws UcanaccessSQLException {
        return replace(expression, find, replacement, start, -1, -1);
    }

    /**
     * Replaces at most the given number of occurrences of a substring, returning the string from the start position on.
     */
    @FunctionType(namingConflict = true, functionName = "Replace", argumentTypes = {AccessType.MEMO, AccessType.MEMO, AccessType.MEMO, AccessType.LONG, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String replace(String expression, String find, String replacement, Integer start, Integer count) throws UcanaccessSQLException {
        return replace(expression, find, replacement, start, count, -1);
    }

    /**
     * Replaces occurrences of a substring like the Access function Replace.
     *
     * @param expression  string to search in, if {@code null}, {@code null} is returned
     * @param find        substring to replace
     * @param replacement replacement string
     * @param start       position to start at, the part before it is left out of the result
     * @param count       maximum number of replacements, -1 for all
     * @param compare     0 for binary comparison, -1, 1 or 2 for text comparison ignoring case
     * @return string from the start position on with the replacements made
     * @throws UcanaccessSQLException if start, count or compare are invalid
     */
    @FunctionType(namingConflict = true, functionName = "Replace",
        argumentTypes = {AccessType.MEMO, AccessType.MEMO, AccessType.MEMO, AccessType.LONG, AccessType.LONG, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String replace(String expression, String find, String replacement, Integer start, Integer count, Integer compare)
        throws UcanaccessSQLException {
        if (expression == null || find == null || replacement == null || start == null || count == null || compare == null) {
            return null;
        } else if (start < 1) {
            throw new InvalidFunctionParameterException("Replace", start);
        } else if (count < -1) {
            throw new InvalidFunctionParameterException("Replace", count);
        } else if (compare < -1 || compare > 2) {
            throw new InvalidFunctionParameterException("Replace", compare);
        } else if (start > expression.length()) {
            return "";
        }
        String text = expression.substring(start - 1);
        if (find.isEmpty() || count == 0) {
            return text;
        }
        boolean ignoreCase = compare != 0;
        StringBuilder sb = new StringBuilder(text.length());
        int replaced = 0;
        int pos = 0;
        int i = 0;
        while (i <= text.length() - find.length() && (count == -1 || replaced < count)) {
            if (text.regionMatches(ignoreCase, i, find, 0, find.length())) {
                sb.append(text, pos, i).append(replacement);
                i += find.length();
                pos = i;
                replaced++;
            } else {
                i++;
            }
        }
        return sb.append(text, pos, text.length()).toString();
    }

    /**
     * Returns a string converted as specified.
     */
    @FunctionType(functionName = "StrConv", argumentTypes = {AccessType.MEMO, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String strConv(String value, int conversion) {
        if (value == null) {
            return null;
        } else if (conversion == 1) { // vbUpperCase
            return value.toUpperCase(Locale.getDefault());
        } else if (conversion == 2) { // vbLowerCase
            return value.toLowerCase(Locale.getDefault());
        } else if (conversion == 3) { // vbProperCase
            return properCase(value);
        }
        return value;
    }

    /**
     * Converts the first letter of every word to upper case and all other letters to lower case.
     */
    private static String properCase(String value) {
        StringBuilder sb = new StringBuilder(value.length());
        boolean wordStart = true;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            sb.append(wordStart ? Character.toUpperCase(c) : Character.toLowerCase(c));
            wordStart = Character.isWhitespace(c) || c == '\0';
        }
        return sb.toString();
    }

    /**
     * Returns an integer value representing the result of a string comparison.
     */
    @FunctionType(functionName = "StrComp", argumentTypes = {AccessType.MEMO, AccessType.MEMO, AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer strComp(String value1, String value2, Integer type) throws UcanaccessSQLException {
        if (value1 == null || value2 == null) {
            return null;
        }
        switch (type) {
            case 0:
                return Integer.signum(value1.compareTo(value2));
            case -1:
            case 1:
            case 2:
                return Integer.signum(textCollator().compare(value1, value2));
            default:
                throw new InvalidFunctionParameterException("StrComp", type);
        }
    }

    /**
     * Returns a collator for text comparison like in Access, which ignores case but not accents.
     *
     * @return new collator for the default locale
     */
    private static Collator textCollator() {
        Collator collator = Collator.getInstance();
        collator.setStrength(Collator.SECONDARY);
        return collator;
    }

    /**
     * Compares two strings like Access queries do by default, that is ignoring case.
     */
    @FunctionType(functionName = "StrComp", argumentTypes = {AccessType.MEMO, AccessType.MEMO}, returnType = AccessType.LONG)
    public static Integer strComp(String value1, String value2) throws UcanaccessSQLException {
        return strComp(value1, value2, -1);
    }

    /**
     * Returns the integer portion of a number, rounding negative numbers down like Access. The result is a 64-bit
     * integer, so unlike a long integer it also holds values beyond 2147483647, as Access does.
     */
    @FunctionType(functionName = "Int", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.BIGINT)
    public static Long mint(Double value) {
        return value == null ? null : BigDecimal.valueOf(Math.floor(value)).longValueExact();
    }

    @FunctionType(functionName = "Int", argumentTypes = {AccessType.YESNO}, returnType = AccessType.INTEGER)
    public static Short mint(boolean value) {
        return (short) (value ? -1 : 0);
    }

    @FunctionType(functionName = "DDB", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double ddb(double cost, double salvage, double life, double period) {
        return ddb(cost, salvage, life, period, 2d);
    }

    @FunctionType(functionName = "DDB", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double ddb(double cost, double salvage, double life, double period, double factor) {
        if (cost < 0 || (life == 2d && period > 1d)) {
            return 0;
        }
        if (life < 2d || (life == 2d && period <= 1d)) {
            return cost - salvage;
        }
        if (period <= 1d) {
            return Math.min(cost * factor / life, cost - salvage);
        }
        double retk = Math.max(salvage - cost * Math.pow((life - factor) / life, period), 0);

        return Math.max(factor * cost / life * Math.pow((life - factor) / life, period - 1d) - retk, 0);
    }

    @FunctionType(functionName = "FV", argumentTypes = {AccessType.DOUBLE, AccessType.LONG, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double fv(double rate, int periods, double payment) {
        return fv(rate, periods, payment, 0, 0);
    }

    @FunctionType(functionName = "FV", argumentTypes = {AccessType.DOUBLE, AccessType.LONG, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double fv(double rate, int periods, double payment, double pv) {
        return fv(rate, periods, payment, pv, 0);
    }

    @FunctionType(functionName = "FV", argumentTypes = {AccessType.DOUBLE, AccessType.LONG, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double fv(double rate, int periods, double payment, double pv, double type) {
        type = Math.abs(type) >= 1 ? 1 : 0;
        double fv = pv * Math.pow(1 + rate, periods);
        for (int i = 0; i < periods; i++) {
            fv += payment * Math.pow(1 + rate, i + type);
        }
        return -fv;
    }

    @FunctionType(functionName = "PMT", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double pmt(double rate, double periods, double pv) {
        return pmt(rate, periods, pv, 0, 0);
    }

    @FunctionType(functionName = "PMT", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double pmt(double rate, double periods, double pv, double fv) {
        return pmt(rate, periods, pv, fv, 0);
    }

    @FunctionType(functionName = "PMT", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double pmt(double rate, double periods, double pv, double fv, double type) {
        type = Math.abs(type) >= 1 ? 1 : 0;

        if (rate == 0) {
            return -1 * (fv + pv) / periods;
        } else {
            return (fv + pv * Math.pow(1 + rate, periods)) * rate
                / ((type == 1 ? 1 + rate : 1) * (1 - Math.pow(1 + rate, periods)));
        }

    }

    @FunctionType(functionName = "NPER", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double nper(double rate, double pmt, double pv) {
        return nper(rate, pmt, pv, 0, 0);
    }

    @FunctionType(functionName = "NPER", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double nper(double rate, double pmt, double pv, double fv) {

        return nper(rate, pmt, pv, fv, 0);
    }

    @FunctionType(functionName = "NPER", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double nper(double rate, double pmt, double pv, double fv, double type) {
        type = Math.abs(type) >= 1 ? 1 : 0;
        double nper = 0;
        if (rate == 0) {
            nper = -1 * (fv + pv) / pmt;
        } else {

            double cr = (type == 1 ? 1 + rate : 1) * pmt / rate;
            double val1 = cr - fv < 0 ? Math.log(fv - cr) : Math.log(cr - fv);
            double val2 = cr - fv < 0 ? Math.log(-pv - cr) : Math.log(pv + cr);
            double val3 = Math.log(1 + rate);
            nper = (val1 - val2) / val3;
        }
        return nper;
    }

    @FunctionType(functionName = "IPMT", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double ipmt(double rate, double per, double nper, double pv) {
        return ipmt(rate, per, nper, pv, 0, 0);
    }

    @FunctionType(functionName = "IPMT", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double ipmt(double rate, double per, double nper, double pv, double fv) {
        return ipmt(rate, per, nper, pv, fv, 0);
    }

    @FunctionType(functionName = "IPMT",
        argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double ipmt(double rate, double per, double nper, double pv, double fv, double type) {
        type = Math.abs(type) >= 1 ? 1 : 0;
        double ipmt = fv(rate, (int) per - 1, pmt(rate, nper, pv, fv, type), pv, type) * rate;
        if (type == 1) {
            ipmt = ipmt / (1 + rate);
        }
        return ipmt;
    }

    @FunctionType(functionName = "PV", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double pv(double rate, double nper, double pmt) {
        return pv(rate, nper, pmt, 0, 0);

    }

    @FunctionType(functionName = "PV", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double pv(double rate, double nper, double pmt, double fv) {
        return pv(rate, nper, pmt, fv, 0);

    }

    @FunctionType(functionName = "PV", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double pv(double rate, double nper, double pmt, double fv, double type) {
        type = Math.abs(type) >= 1 ? 1 : 0;

        if (rate == 0) {
            return -1 * (nper * pmt + fv);
        } else {

            return ((1 - Math.pow(1 + rate, nper)) / rate * (type == 1 ? 1 + rate : 1) * pmt - fv)
                / Math.pow(1 + rate, nper);
        }

    }

    @FunctionType(functionName = "PPMT", argumentTypes = {AccessType.DOUBLE, AccessType.LONG, AccessType.LONG, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double ppmt(double rate, int per, int nper, double pv) {
        return ppmt(rate, per, nper, pv, 0, 0);
    }

    @FunctionType(functionName = "PPMT", argumentTypes = {AccessType.DOUBLE, AccessType.LONG, AccessType.LONG, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double ppmt(double rate, int per, int nper, double pv, double fv) {
        return ppmt(rate, per, nper, pv, fv, 0);
    }

    @FunctionType(functionName = "PPMT", argumentTypes = {AccessType.DOUBLE, AccessType.LONG, AccessType.LONG, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double ppmt(double rate, int per, int nper, double pv, double fv, double type) {
        return pmt(rate, nper, pv, fv, type) - ipmt(rate, per, nper, pv, fv, type);
    }

    @FunctionType(functionName = "SLN", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double sln(double cost, double salvage, double life) {
        return (cost - salvage) / life;
    }

    @FunctionType(functionName = "SYD", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double syd(double cost, double salvage, double life, double per) {
        return (cost - salvage) * (life - per + 1) * 2 / (life * (life + 1));
    }

    @FunctionType(functionName = "RATE", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double rate(double nper, double pmt, double pv) {
        return rate(nper, pmt, pv, 0, 0, 0.1);
    }

    @FunctionType(functionName = "RATE", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double rate(double nper, double pmt, double pv, double fv) {
        return rate(nper, pmt, pv, fv, 0, 0.1);
    }

    @FunctionType(functionName = "RATE", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double rate(double nper, double pmt, double pv, double fv, double type) {
        return rate(nper, pmt, pv, fv, type, 0.1);
    }

    @FunctionType(functionName = "RATE",
        argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double rate(double nper, double pmt, double pv, double fv, double type, double guess) {
        // FROM MS http://office.microsoft.com/en-us/excel-help/rate-HP005209232.aspx

        type = Math.abs(type) >= 1 ? 1 : 0; // the only change to the implementation Apache POI
        final int financialMaxIterations = 20; // Bet accuracy with 128
        final double financialPrecision = 0.0000001; // 1.0e-8

        // the secant method starts from rates 0 and guess, so a guess of 0 gives no second point
        double rate = Math.abs(guess) < financialPrecision ? 0.1 : guess;
        double f = Math.exp(nper * Math.log(1 + rate));
        double y0 = pv + pmt * nper + fv;
        double y1 = pv * f + pmt * (1 / rate + type) * (f - 1) + fv;

        // find root by Newton secant method
        double i = 0.0;
        double x0 = 0.0;
        double x1 = rate;
        while (Math.abs(y0 - y1) > financialPrecision && i < financialMaxIterations) {
            rate = (y1 * x0 - y0 * x1) / (y1 - y0);
            x0 = x1;
            x1 = rate;

            double y;
            if (Math.abs(rate) < financialPrecision) {
                y = pv * (1 + nper * rate) + pmt * (1 + rate * type) * nper + fv;
            } else {
                f = Math.exp(nper * Math.log(1 + rate));
                y = pv * f + pmt * (1 / rate + type) * (f - 1) + fv;
            }

            y0 = y1;
            y1 = y;
            ++i;
        }

        return rate;
    }

    @FunctionType(functionName = "formulaToNumeric", argumentTypes = {AccessType.DOUBLE, AccessType.MEMO}, returnType = AccessType.DOUBLE)
    public static Double formulaToNumeric(Double res, String datatype) {
        return res;
    }

    @FunctionType(functionName = "formulaToNumeric", argumentTypes = {AccessType.YESNO, AccessType.MEMO}, returnType = AccessType.DOUBLE)
    public static Double formulaToNumeric(Boolean res, String datatype) {
        if (res == null) {
            return null;
        }
        return res ? -1d : 0d;
    }

    @FunctionType(functionName = "formulaToNumeric", argumentTypes = {AccessType.MEMO, AccessType.MEMO}, returnType = AccessType.DOUBLE)
    public static Double formulaToNumeric(String input, String datatype) {
        if (input == null) {
            return null;
        }
        return Try.catching(() -> {
            DecimalFormatSymbols dfs = DecimalFormatSymbols.getInstance();
            String sep = dfs.getDecimalSeparator() + "";
            String gs = dfs.getGroupingSeparator() + "";
            String res = input.replaceAll(Pattern.quote(gs), "");
            if (!".".equalsIgnoreCase(sep)) {
                res = res.replaceAll(Pattern.quote(sep), ".");
            }
            double d = val(res);
            DataType dt = DataType.valueOf(datatype);
            if (dt.equals(DataType.BYTE) || dt.equals(DataType.INT) || dt.equals(DataType.LONG)) {
                d = Math.rint(d + APPROX);
            }
            return d;
        }).orIgnore();
    }

    @FunctionType(functionName = "formulaToNumeric", argumentTypes = {AccessType.DATETIME, AccessType.MEMO}, returnType = AccessType.DOUBLE)
    public static Double formulaToNumeric(Timestamp res, String datatype) throws UcanaccessSQLException {
        if (res == null) {
            return null;
        }
        return (double) dateDiff("y", Timestamp.valueOf(ACCESS_BASE_DATE), res);
    }

    @FunctionType(functionName = "formulaToBoolean", argumentTypes = {AccessType.YESNO, AccessType.MEMO}, returnType = AccessType.YESNO)
    public static Boolean formulaToBoolean(Boolean res, String datatype) {
        return res;
    }

    @FunctionType(functionName = "formulaToBoolean", argumentTypes = {AccessType.DOUBLE, AccessType.MEMO}, returnType = AccessType.YESNO)
    public static Boolean formulaToBoolean(Double res, String datatype) {
        return res == null ? null : res != 0d;
    }

    @SuppressWarnings("java:S2447") // null is the SQL NULL result of this function
    @FunctionType(functionName = "formulaToBoolean", argumentTypes = {AccessType.DATETIME, AccessType.MEMO}, returnType = AccessType.YESNO)
    public static Boolean formulaToBoolean(Timestamp res, String datatype) {
        return null;
    }

    @SuppressWarnings("java:S2447") // null is the SQL NULL result of this function
    @FunctionType(functionName = "formulaToBoolean", argumentTypes = {AccessType.MEMO, AccessType.MEMO}, returnType = AccessType.YESNO)
    public static Boolean formulaToBoolean(String res, String datatype) {
        if (res == null) {
            return null;
        } else if ("-1".equals(res)) {
            return true;
        } else if ("0".equals(res)) {
            return false;
        }
        return null;
    }

    @FunctionType(functionName = "formulaToText", argumentTypes = {AccessType.MEMO, AccessType.MEMO}, returnType = AccessType.MEMO)
    public static String formulaToText(String res, String datatype) {
        return res;
    }

    @FunctionType(functionName = "formulaToText", argumentTypes = {AccessType.DOUBLE, AccessType.MEMO}, returnType = AccessType.MEMO)
    public static String formulaToText(Double res, String datatype) {
        if (res == null) {
            return null;
        }
        DecimalFormatSymbols dfs = DecimalFormatSymbols.getInstance();
        DecimalFormat df = new DecimalFormat("#", dfs);
        df.setGroupingUsed(false);
        df.setMaximumFractionDigits(100);
        return df.format(res);
    }

    @FunctionType(functionName = "formulaToText", argumentTypes = {AccessType.YESNO, AccessType.MEMO}, returnType = AccessType.MEMO)
    public static String formulaToText(Boolean res, String datatype) {
        if (res == null) {
            return null;
        }
        return res ? "-1" : "0";
    }

    @FunctionType(functionName = "formulaToText", argumentTypes = {AccessType.DATETIME, AccessType.MEMO}, returnType = AccessType.MEMO)
    public static String formulaToText(Timestamp res, String datatype) throws UcanaccessSQLException {
        LocalDateTime ldt = res.toLocalDateTime();
        if (ldt.getHour() == 0 && ldt.getMinute() == 0 && ldt.getSecond() == 0) {
            return format(res, "short date");
        } else {
            return format(res, GENERAL_DATE);
        }
    }

    @FunctionType(functionName = "formulaToDate", argumentTypes = {AccessType.DATETIME, AccessType.MEMO}, returnType = AccessType.DATETIME)
    public static Timestamp formulaToDate(Timestamp res, String datatype) {
        return res;
    }

    @FunctionType(functionName = "formulaToDate", argumentTypes = {AccessType.MEMO, AccessType.MEMO}, returnType = AccessType.DATETIME)
    public static Timestamp formulaToDate(String res, String datatype) {
        if (res == null) {
            return null;
        }
        return Try.catching(() -> dateValue(res, false)).orIgnore();
    }

    @FunctionType(functionName = "formulaToDate", argumentTypes = {AccessType.YESNO, AccessType.MEMO}, returnType = AccessType.DATETIME)
    public static Timestamp formulaToDate(Boolean res, String datatype) throws UcanaccessSQLException {
        if (res == null) {
            return null;
        }
        return dateAdd("y", res ? -1 : 0, Timestamp.valueOf(ACCESS_BASE_DATE));
    }

    @FunctionType(functionName = "orderJet", argumentTypes = {AccessType.MEMO}, returnType = AccessType.MEMO)
    public static String orderJet(String s) {
        return s.replaceAll("([a-zA-Z0-9])[\\-–—]([a-zA-Z0-9])", "$1$2");
    }

    @FunctionType(functionName = "formulaToDate", argumentTypes = {AccessType.DOUBLE, AccessType.MEMO}, returnType = AccessType.DATETIME)
    public static Timestamp formulaToDate(Double res, String datatype) throws UcanaccessSQLException {
        if (res == null) {
            return null;
        }
        Double d = Math.floor(res);
        Timestamp tr = dateAdd("y", d.intValue(), Timestamp.valueOf(ACCESS_BASE_DATE));
        d = (res - res.intValue()) * 24;
        tr = dateAdd("H", d.intValue(), tr);
        d = (d - d.intValue()) * 60;
        tr = dateAdd("N", d.intValue(), tr);
        d = (d - d.intValue()) * 60;
        tr = dateAdd("S", (int) Math.rint(d + APPROX), tr);
        return tr;
    }

    /**
     * Extracts a substring from a string starting from the right-most character.
     */
    @FunctionType(functionName = "Right", namingConflict = true, argumentTypes = {AccessType.MEMO, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String right(String input, int i) {
        if (input == null || i < 0) {
            return null;
        }
        int ln = input.length();
        if (i >= ln) {
            return input;
        } else {
            return input.substring(ln - i, ln);
        }
    }

    @FunctionType(functionName = "\"RIGHT$\"", argumentTypes = {AccessType.MEMO, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String rightS(String input, int i) {
        return right(input, i);
    }

    /**
     * Returns a number rounded to a whole number, rounding halves to the nearest even number like Access.
     */
    @FunctionType(namingConflict = true, functionName = "Round", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double round(double d) {
        return round(d, 0);
    }

    /**
     * Returns a number rounded to a specified number of decimal places. Like Access, halves are rounded to the nearest
     * even digit.
     */
    @FunctionType(namingConflict = true, functionName = "Round", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double round(double d, double p) {
        if (!Double.isFinite(d)) {
            return d;
        }
        return BigDecimal.valueOf(d).setScale((int) p, RoundingMode.HALF_EVEN).doubleValue();
    }

    /**
     * Returns the integer portion of a number.
     *
     * @param number a double or any valid numeric expression, if {@code null}, {@code null} is returned
     * @return integer portion
     */
    @FunctionType(namingConflict = true, functionName = "Fix", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static Double fix(Double number) {
        return number == null ? null : number < 0 ? Math.ceil(number) : Math.floor(number);
    }

    @FunctionType(functionName = "PARTITION", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.MEMO)
    public static String partition(Double number, double start, double stop, double interval) {
        if (number == null) {
            return null;
        }
        number = Math.rint(number);
        interval = Math.rint(interval);

        String ul = String.valueOf(lrint(stop) + 1);
        stop = lrint(stop);
        start = lrint(start);
        int h = ul.length();
        if (number < start) {
            return padLeft("", h) + ":" + padLeft(lrint(start) - 1, h);
        }
        if (number > stop) {
            return ul + ":" + padLeft("", h);
        }

        for (double d = start; d <= stop; d += interval) {
            if (number >= d && number < d + interval) {
                return padLeft(lceil(d), h) + ":"
                    + padLeft(d + interval <= stop ? lfloor(d + interval) : lrint(stop), h);

            }
        }
        return "";
    }

    private static int lfloor(double d) {
        return (int) Math.floor(d - APPROX);
    }

    private static int lceil(double d) {
        return (int) Math.ceil(d - APPROX);
    }

    private static int lrint(double d) {
        return (int) Math.rint(d - APPROX);
    }

    private static String padLeft(int value, int n) {
        return padLeft(String.valueOf(value), n);
    }

    private static String padLeft(String value, int n) {
        return String.format("%1$" + n + "s", value);
    }

}
