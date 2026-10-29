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

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.security.SecureRandom;
import java.sql.Timestamp;
import java.text.*;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjuster;
import java.time.temporal.TemporalAdjusters;
import java.time.temporal.WeekFields;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Functions {
    private static SecureRandom random;
    private static Double       rnd;
    private static Double       lastRnd;
    private static final double APPROX = 0.00000001;

    private static final String GENERAL_DATE = "general date";

    /** Day zero of Access date values. */
    private static final LocalDateTime ACCESS_BASE_DATE = LocalDateTime.of(1899, 12, 30, 0, 0);

    /** Hexadecimal ({@code &H}) or octal ({@code &O} or just {@code &}) number at the start of a string, as in Val. */
    private static final Pattern VAL_RADIX            = Pattern.compile("&(?:[Hh]([0-9A-Fa-f]*)|[Oo]?([0-7]*))");
    /** Time of day with optional minutes, seconds and AM/PM, as in TimeValue. */
    private static final Pattern TIME_VALUE           = Pattern.compile("(\\d{1,2})(?::(\\d{1,2})(?::(\\d{1,2}))?)?\\s*([AaPp][Mm]?)?");
    /** Time without seconds at the end of a date string, optionally with AM or PM. */
    private static final Pattern TIME_WITHOUT_SECONDS = Pattern.compile("(?<![:\\d])(\\d{1,2}:\\d{1,2})(\\s*(?:[AaPp][Mm])?)$");
    /** Decimal number with optional exponent at the start of a string, as in Val. */
    private static final Pattern VAL_DECIMAL          = Pattern.compile("([+-]?\\d*(?:\\.\\d*)?)([eEdD][+-]?\\d+)?");

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

    /**
     * Returns the Unicode code of the first character of a string. Like in VBA, the result is a 16-bit integer, so
     * codes above 32767 are negative.
     */
    @FunctionType(functionName = "AscW", argumentTypes = {AccessType.MEMO}, returnType = AccessType.LONG)
    public static Integer ascW(String s) {
        return s == null || s.isEmpty() ? null : (int) (short) s.charAt(0);
    }

    /**
     * Returns the character with the given Unicode code. Negative codes down to -32768 are taken as 16-bit integers,
     * as AscW returns them.
     */
    @FunctionType(functionName = "ChrW", argumentTypes = {AccessType.LONG}, returnType = AccessType.MEMO)
    public static String chrW(Integer code) {
        if (code == null) {
            return null;
        } else if (code < Short.MIN_VALUE || code > 0xFFFF) {
            UcanaccessRuntimeException.throwNow(String.format("Invalid character code for function ChrW: %d", code));
        }
        return String.valueOf((char) (code & 0xFFFF));
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
            return obj instanceof Number && ((Number) obj).doubleValue() != 0;
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
    @FunctionType(functionName = "CCur", argumentTypes = {AccessType.NUMERIC}, returnType = AccessType.CURRENCY)
    public static BigDecimal ccur(BigDecimal value) {
        return value == null ? null : value.setScale(4, RoundingMode.HALF_EVEN);
    }

    /**
     * Converts a floating point number into a currency value, rounding halves to the even digit like Access.
     */
    @FunctionType(functionName = "CCur", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.CURRENCY)
    public static BigDecimal ccur(Double value) {
        return value == null ? null : BigDecimal.valueOf(value).setScale(4, RoundingMode.HALF_EVEN);
    }

    /**
     * Converts an expression into a date value.
     */
    @FunctionType(functionName = "CDate", argumentTypes = {AccessType.MEMO}, returnType = AccessType.DATETIME)
    public static Timestamp cdate(String dt) {
        return dateValue(dt, false);
    }

    /**
     * Returns a date unchanged.
     */
    @FunctionType(functionName = "CDate", argumentTypes = {AccessType.DATETIME}, returnType = AccessType.DATETIME)
    public static Timestamp cdate(Timestamp dt) {
        return dt;
    }

    /**
     * Returns a date unchanged, like CDate.
     */
    @FunctionType(functionName = "CVDate", argumentTypes = {AccessType.DATETIME}, returnType = AccessType.DATETIME)
    public static Timestamp cvdate(Timestamp dt) {
        return dt;
    }

    /**
     * Converts an expression into a date value, like CDate.
     */
    @FunctionType(functionName = "CVDate", argumentTypes = {AccessType.MEMO}, returnType = AccessType.DATETIME)
    public static Timestamp cvdate(String dt) {
        return cdate(dt);
    }

    /**
     * Converts an Access date value to a date, like CDate.
     */
    @FunctionType(functionName = "CVDate", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.DATETIME)
    public static Timestamp cvdate(Double value) {
        return cdate(value);
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
        return csng(value);
    }

    /**
     * Converts an expression to a single precision number, which has about seven significant digits.
     */
    @FunctionType(functionName = "CSng", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.SINGLE)
    public static Double csng(Double value) {
        return value == null ? null : new BigDecimal(value.toString(), new MathContext(7)).doubleValue();
    }

    /**
     * Converts an expression to a byte from 0 to 255, rounding halves to the nearest even number like Access.
     */
    @FunctionType(functionName = "CByte", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.BYTE)
    public static Short cbyte(Double value) {
        if (value == null) {
            return null;
        }
        double d = Math.rint(value);
        if (d < 0 || d > 255) {
            UcanaccessRuntimeException.throwNow(String.format("Overflow in function CByte: %s", value));
        }
        return (short) d;
    }

    /**
     * Converts a boolean to text like in Access queries, where True is -1 and False is 0.
     */
    @FunctionType(functionName = "CStr", argumentTypes = {AccessType.YESNO}, returnType = AccessType.MEMO)
    public static String cstr(Boolean value) {
        return value == null ? null : value ? "-1" : "0";
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
    public static Integer dateDiff(String intv, Timestamp dt1, Timestamp dt2) throws UcanaccessSQLException {
        return dateDiff(intv, dt1, dt2, 1);
    }

    /**
     * Returns the number of interval boundaries between two dates, like Access. For example, the difference in
     * minutes between 00:00:59 and 00:01:00 is 1, and the difference in weeks (ww) counts the first days of the week
     * in between. Only the interval w counts whole periods of seven days.
     *
     * @param intv           interval: yyyy, q, m, y, d, w, ww, h, n or s
     * @param dt1            first date
     * @param dt2            second date, the result is negative if it is before the first date
     * @param firstDayOfWeek 1 for Sunday to 7 for Saturday, 0 for the default locale, only used for ww
     * @return number of intervals, {@code null} if an argument is {@code null}
     * @throws UcanaccessSQLException on an invalid interval or first day of week
     */
    @FunctionType(namingConflict = true, functionName = "DateDiff",
        argumentTypes = {AccessType.MEMO, AccessType.DATETIME, AccessType.DATETIME, AccessType.LONG}, returnType = AccessType.LONG)
    @SuppressWarnings("java:S8700") // Access dates have no time zone, so the difference is measured in wall-clock time
    public static Integer dateDiff(String intv, Timestamp dt1, Timestamp dt2, Integer firstDayOfWeek) throws UcanaccessSQLException {
        if (dt1 == null || intv == null || dt2 == null || firstDayOfWeek == null) {
            return null;
        } else if (firstDayOfWeek < 0 || firstDayOfWeek > 7) {
            throw new InvalidFunctionParameterException("DateDiff", firstDayOfWeek);
        }
        LocalDateTime from = dt1.toLocalDateTime();
        LocalDateTime to = dt2.toLocalDateTime();
        long result;
        if ("yyyy".equalsIgnoreCase(intv)) {
            result = (long) to.getYear() - from.getYear();
        } else if ("q".equalsIgnoreCase(intv)) {
            result = (to.getYear() - from.getYear()) * 4L + (to.getMonthValue() - 1) / 3 - (from.getMonthValue() - 1) / 3;
        } else if ("m".equalsIgnoreCase(intv)) {
            result = (to.getYear() - from.getYear()) * 12L + to.getMonthValue() - from.getMonthValue();
        } else if ("y".equalsIgnoreCase(intv) || "d".equalsIgnoreCase(intv)) {
            result = ChronoUnit.DAYS.between(from.toLocalDate(), to.toLocalDate());
        } else if ("w".equalsIgnoreCase(intv)) {
            result = ChronoUnit.DAYS.between(from.toLocalDate(), to.toLocalDate()) / 7;
        } else if ("ww".equalsIgnoreCase(intv)) {
            DayOfWeek firstDay = firstDayOfWeek == 0
                ? WeekFields.of(Locale.getDefault()).getFirstDayOfWeek()
                : DayOfWeek.SUNDAY.plus(firstDayOfWeek - 1L);
            TemporalAdjuster weekStart = TemporalAdjusters.previousOrSame(firstDay);
            result = ChronoUnit.WEEKS.between(from.toLocalDate().with(weekStart), to.toLocalDate().with(weekStart));
        } else if ("h".equalsIgnoreCase(intv)) {
            result = ChronoUnit.HOURS.between(from.truncatedTo(ChronoUnit.HOURS), to.truncatedTo(ChronoUnit.HOURS));
        } else if ("n".equalsIgnoreCase(intv)) {
            result = ChronoUnit.MINUTES.between(from.truncatedTo(ChronoUnit.MINUTES), to.truncatedTo(ChronoUnit.MINUTES));
        } else if ("s".equalsIgnoreCase(intv)) {
            result = ChronoUnit.SECONDS.between(from.truncatedTo(ChronoUnit.SECONDS), to.truncatedTo(ChronoUnit.SECONDS));
        } else {
            throw new InvalidIntervalValueException(intv);
        }
        return Math.toIntExact(result);
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
        if ("ww".equalsIgnoreCase(intv) && dt != null) {
            return weekOfYear(dt.toLocalDateTime().toLocalDate(), firstDayOfWeek, firstWeekOfYear);
        }
        return datePart(intv, dt);
    }

    /**
     * Returns the week of the year like Access. Week 1 is the week with January 1st (firstWeekOfYear 1), the first
     * week with at least four days (2) or the first full week (3) of the year. Days before week 1 belong to the last
     * week of the previous year. Days at the end of the year belong to week 1 of the next year only for 2 and 3, as
     * Access counts week 53 then instead for 1.
     *
     * @param date            date to get the week for
     * @param firstDayOfWeek  1 for Sunday to 7 for Saturday, 0 for the default locale
     * @param firstWeekOfYear 1, 2 or 3 as described above, 0 for the default locale
     * @return week of the year from 1 to 54
     * @throws UcanaccessSQLException if a constant is out of range
     */
    private static int weekOfYear(LocalDate date, Integer firstDayOfWeek, Integer firstWeekOfYear) throws UcanaccessSQLException {
        if (firstDayOfWeek == null || firstDayOfWeek < 0 || firstDayOfWeek > 7) {
            throw new InvalidFunctionParameterException("DatePart", firstDayOfWeek);
        } else if (firstWeekOfYear == null || firstWeekOfYear < 0 || firstWeekOfYear > 3) {
            throw new InvalidFunctionParameterException("DatePart", firstWeekOfYear);
        }
        WeekFields system = WeekFields.of(Locale.getDefault());
        DayOfWeek firstDay = firstDayOfWeek == 0 ? system.getFirstDayOfWeek() : DayOfWeek.SUNDAY.plus(firstDayOfWeek - 1L);
        int fwoy = firstWeekOfYear == 0 ? (system.getMinimalDaysInFirstWeek() >= 4 ? 2 : 1) : firstWeekOfYear;
        int minDays = fwoy == 1 ? 1 : fwoy == 2 ? 4 : 7;
        if (fwoy != 1 && !date.isBefore(firstWeekStart(date.getYear() + 1, firstDay, minDays))) {
            return 1;
        }
        LocalDate start = firstWeekStart(date.getYear(), firstDay, minDays);
        if (date.isBefore(start)) {
            start = firstWeekStart(date.getYear() - 1, firstDay, minDays);
        }
        return (int) ChronoUnit.DAYS.between(start, date) / 7 + 1;
    }

    /**
     * Returns the first day of week 1 of a year, that is of the first week with at least the given days in the year.
     */
    private static LocalDate firstWeekStart(int year, DayOfWeek firstDay, int minDays) {
        LocalDate jan1 = LocalDate.of(year, 1, 1);
        LocalDate start = jan1.with(TemporalAdjusters.previousOrSame(firstDay));
        return 7 - ChronoUnit.DAYS.between(start, jan1) < minDays ? start.plusWeeks(1) : start;
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
            return weekOfYear(ldt.toLocalDate(), 1, 1);
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
     * Returns a date for a specified year, month, and day. Like in Access, the arguments are rounded to whole numbers,
     * month and day may lie outside their usual range and then roll over, and a year from 0 to 99 means 2000 to 2049
     * or 1950 to 1999, the default of Windows.
     *
     * @param year  year from 100 to 9999, or 0 to 99 for a two-digit year
     * @param month month, rolling over into other years
     * @param day   day, rolling over into other months
     * @return date, {@code null} if an argument is {@code null}
     */
    @FunctionType(functionName = "DateSerial", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DATETIME)
    public static Timestamp dateSerial(Double year, Double month, Double day) {
        if (year == null || month == null || day == null) {
            return null;
        }
        long y = (long) Math.rint(year);
        if (y >= 0 && y <= 99) {
            y += y < 50 ? 2000 : 1900;
        }
        LocalDate date = LocalDate.of(1, 1, 1).plusYears(y - 1)
            .plusMonths((long) Math.rint(month) - 1)
            .plusDays((long) Math.rint(day) - 1);
        if (date.getYear() < 100 || date.getYear() > 9999) {
            UcanaccessRuntimeException.throwNow(String.format("Date out of range in function DateSerial: %s", date));
        }
        return Timestamp.valueOf(date.atStartOfDay());
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
        if (dt == null) {
            return null;
        }
        String s = dt.trim();
        if (isTime(TIME_VALUE.matcher(s))) {
            // a time alone is a time on the base date, as in Access
            return onlyDate ? Timestamp.valueOf(ACCESS_BASE_DATE) : timeValue(s);
        }
        Timestamp t = parseDate(s, onlyDate);
        if (t == null && TIME_WITHOUT_SECONDS.matcher(s).find()) {
            t = parseDate(TIME_WITHOUT_SECONDS.matcher(s).replaceFirst("$1:00$2"), onlyDate);
        }
        return t;
    }

    /**
     * Parses a date with the first date format of the regional settings that matches the whole string.
     *
     * @param dt       string to parse
     * @param onlyDate whether to drop the time
     * @return date, or {@code null} if no format matches
     */
    private static Timestamp parseDate(String dt, boolean onlyDate) {
        RegionalSettings reg = getRegionalSettings();
        if (!"true".equalsIgnoreCase(reg.getRS())
            && (!"PM".equalsIgnoreCase(reg.getPM()) || !"AM".equalsIgnoreCase(reg.getAM()))) {
            dt = dt.replaceAll("(?i)" + Pattern.quote(reg.getPM()), "PM")
                .replaceAll("(?i)" + Pattern.quote(reg.getAM()), "AM");
        }

        for (Entry<SimpleDateFormat, Boolean> entry : reg.getDateFormats().entrySet()) {
            SimpleDateFormat sdf = entry.getKey();
            boolean yearOverride = entry.getValue();

            ParsePosition pos = new ParsePosition(0);
            Date parsed = sdf.parse(dt, pos);
            if (parsed != null && pos.getIndex() == dt.length()) {
                Timestamp t = new Timestamp(parsed.getTime());
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
            // like Access, leave out a midnight time and the base date
            LocalDateTime ldt = t.toLocalDateTime();
            if (ldt.toLocalTime().equals(LocalTime.MIDNIGHT)) {
                return formatDate(t, reg.getShortDatePattern());
            } else if (ldt.toLocalDate().equals(ACCESS_BASE_DATE.toLocalDate())) {
                return formatDate(t, reg.getLongTimePattern());
            }
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
     * Formats a number with the decimal and grouping separators of the default locale, by default with two decimal places.
     */
    @FunctionType(functionName = "FormatNumber", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.MEMO)
    public static String formatNumber(Double value) {
        return formatNumberImpl(value, NumberStyle.NUMBER, -1, -2, -2, -2);
    }

    /**
     * Formats a number with the given number of decimal places.
     */
    @FunctionType(functionName = "FormatNumber", argumentTypes = {AccessType.DOUBLE, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String formatNumber(Double value, Integer digits) {
        return formatNumberImpl(value, NumberStyle.NUMBER, digits, -2, -2, -2);
    }

    /**
     * Formats a number choosing whether to show a leading zero.
     */
    @FunctionType(functionName = "FormatNumber", argumentTypes = {AccessType.DOUBLE, AccessType.LONG, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String formatNumber(Double value, Integer digits, Integer leadingDigit) {
        return formatNumberImpl(value, NumberStyle.NUMBER, digits, leadingDigit, -2, -2);
    }

    /**
     * Formats a number choosing whether to put negative numbers in parentheses.
     */
    @FunctionType(functionName = "FormatNumber", argumentTypes = {AccessType.DOUBLE, AccessType.LONG, AccessType.LONG, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String formatNumber(Double value, Integer digits, Integer leadingDigit, Integer parens) {
        return formatNumberImpl(value, NumberStyle.NUMBER, digits, leadingDigit, parens, -2);
    }

    /**
     * Formats a number choosing whether to group digits.
     */
    @FunctionType(functionName = "FormatNumber", argumentTypes = {AccessType.DOUBLE, AccessType.LONG, AccessType.LONG, AccessType.LONG, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String formatNumber(Double value, Integer digits, Integer leadingDigit, Integer parens, Integer group) {
        return formatNumberImpl(value, NumberStyle.NUMBER, digits, leadingDigit, parens, group);
    }

    /**
     * Formats a number as currency of the default locale, by default with two decimal places.
     */
    @FunctionType(functionName = "FormatCurrency", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.MEMO)
    public static String formatCurrency(Double value) {
        return formatNumberImpl(value, NumberStyle.CURRENCY, -1, -2, -2, -2);
    }

    /**
     * Formats a number as currency with the given number of decimal places.
     */
    @FunctionType(functionName = "FormatCurrency", argumentTypes = {AccessType.DOUBLE, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String formatCurrency(Double value, Integer digits) {
        return formatNumberImpl(value, NumberStyle.CURRENCY, digits, -2, -2, -2);
    }

    /**
     * Formats a number as currency choosing whether to show a leading zero.
     */
    @FunctionType(functionName = "FormatCurrency", argumentTypes = {AccessType.DOUBLE, AccessType.LONG, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String formatCurrency(Double value, Integer digits, Integer leadingDigit) {
        return formatNumberImpl(value, NumberStyle.CURRENCY, digits, leadingDigit, -2, -2);
    }

    /**
     * Formats a number as currency choosing whether to put negative numbers in parentheses.
     */
    @FunctionType(functionName = "FormatCurrency", argumentTypes = {AccessType.DOUBLE, AccessType.LONG, AccessType.LONG, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String formatCurrency(Double value, Integer digits, Integer leadingDigit, Integer parens) {
        return formatNumberImpl(value, NumberStyle.CURRENCY, digits, leadingDigit, parens, -2);
    }

    /**
     * Formats a number as currency choosing whether to group digits.
     */
    @FunctionType(functionName = "FormatCurrency", argumentTypes = {AccessType.DOUBLE, AccessType.LONG, AccessType.LONG, AccessType.LONG, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String formatCurrency(Double value, Integer digits, Integer leadingDigit, Integer parens, Integer group) {
        return formatNumberImpl(value, NumberStyle.CURRENCY, digits, leadingDigit, parens, group);
    }

    /**
     * Formats a number multiplied by 100 with a trailing percent sign, by default with two decimal places.
     */
    @FunctionType(functionName = "FormatPercent", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.MEMO)
    public static String formatPercent(Double value) {
        return formatNumberImpl(value, NumberStyle.PERCENT, -1, -2, -2, -2);
    }

    /**
     * Formats a number as percentage with the given number of decimal places.
     */
    @FunctionType(functionName = "FormatPercent", argumentTypes = {AccessType.DOUBLE, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String formatPercent(Double value, Integer digits) {
        return formatNumberImpl(value, NumberStyle.PERCENT, digits, -2, -2, -2);
    }

    /**
     * Formats a number as percentage choosing whether to show a leading zero.
     */
    @FunctionType(functionName = "FormatPercent", argumentTypes = {AccessType.DOUBLE, AccessType.LONG, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String formatPercent(Double value, Integer digits, Integer leadingDigit) {
        return formatNumberImpl(value, NumberStyle.PERCENT, digits, leadingDigit, -2, -2);
    }

    /**
     * Formats a number as percentage choosing whether to put negative numbers in parentheses.
     */
    @FunctionType(functionName = "FormatPercent", argumentTypes = {AccessType.DOUBLE, AccessType.LONG, AccessType.LONG, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String formatPercent(Double value, Integer digits, Integer leadingDigit, Integer parens) {
        return formatNumberImpl(value, NumberStyle.PERCENT, digits, leadingDigit, parens, -2);
    }

    /**
     * Formats a number as percentage choosing whether to group digits.
     */
    @FunctionType(functionName = "FormatPercent", argumentTypes = {AccessType.DOUBLE, AccessType.LONG, AccessType.LONG, AccessType.LONG, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String formatPercent(Double value, Integer digits, Integer leadingDigit, Integer parens, Integer group) {
        return formatNumberImpl(value, NumberStyle.PERCENT, digits, leadingDigit, parens, group);
    }

    /** Kind of number formatting of FormatNumber, FormatCurrency and FormatPercent. */
    private enum NumberStyle {
        NUMBER, CURRENCY, PERCENT
    }

    /**
     * Formats a number like FormatNumber, FormatCurrency and FormatPercent. The tristate arguments are -1 for true, 0
     * for false and -2 for the default of the regional settings.
     *
     * @param value        number to format, if {@code null}, {@code null} is returned
     * @param style        number, currency or percent
     * @param digits       number of decimal places, -1 for the default of two
     * @param leadingDigit tristate whether to show a zero before the decimal separator, default true
     * @param parens       tristate whether to put negative numbers in parentheses, default false
     * @param group        tristate whether to group digits, default true
     * @return formatted number
     */
    @SuppressWarnings("checkstyle:ParameterNumber")
    private static String formatNumberImpl(Double value, NumberStyle style, Integer digits, Integer leadingDigit, Integer parens,
        Integer group) {
        if (value == null) {
            return null;
        } else if (digits == null || digits < -1) {
            throw new UcanaccessRuntimeException(String.format("Invalid number of decimal places: %s", digits));
        }
        int decimals = digits == -1 ? 2 : digits;
        DecimalFormat df = style == NumberStyle.CURRENCY
            ? (DecimalFormat) NumberFormat.getCurrencyInstance()
            : (DecimalFormat) NumberFormat.getNumberInstance();
        df.setRoundingMode(RoundingMode.HALF_UP);
        df.setMinimumFractionDigits(decimals);
        df.setMaximumFractionDigits(decimals);
        df.setMinimumIntegerDigits(Objects.equals(leadingDigit, 0) ? 0 : 1);
        df.setGroupingUsed(!Objects.equals(group, 0));
        BigDecimal number = BigDecimal.valueOf(value);
        if (style == NumberStyle.PERCENT) {
            number = number.movePointRight(2);
        }
        String text = df.format(number.abs());
        if (style == NumberStyle.CURRENCY) {
            // Java separates the currency symbol with a non-breaking space, Access with a normal one
            text = text.replace('\u00a0', ' ');
        } else if (style == NumberStyle.PERCENT) {
            text += "%";
        }
        if (number.setScale(decimals, RoundingMode.HALF_UP).signum() >= 0) {
            return text;
        }
        return Objects.equals(parens, -1) ? "(" + text + ")" : "-" + text;
    }

    /**
     * Formats a date as general date, showing only the date when there is no time and only the time on the base date.
     */
    @FunctionType(functionName = "FormatDateTime", argumentTypes = {AccessType.DATETIME}, returnType = AccessType.MEMO)
    public static String formatDateTime(Timestamp t) throws UcanaccessSQLException {
        return formatDateTime(t, 0);
    }

    /**
     * Formats a date with a named format: 0 general date, 1 long date, 2 short date, 3 long time, 4 short time.
     *
     * @param t      date to format, if {@code null}, {@code null} is returned
     * @param format named format from 0 to 4
     * @return formatted date
     * @throws UcanaccessSQLException if the named format is invalid
     */
    @FunctionType(functionName = "FormatDateTime", argumentTypes = {AccessType.DATETIME, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String formatDateTime(Timestamp t, Integer format) throws UcanaccessSQLException {
        if (t == null || format == null) {
            return null;
        }
        switch (format) {
            case 0:
                return format(t, GENERAL_DATE);
            case 1:
                return format(t, "long date");
            case 2:
                return format(t, "short date");
            case 3:
                return format(t, "long time");
            case 4:
                return format(t, "HH:nn");
            default:
                throw new InvalidFunctionParameterException("FormatDateTime", format);
        }
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
     * Returns the current system date as text in the format mm-dd-yyyy, like Date$ in Access.
     */
    @FunctionType(functionName = "\"DATE$\"", argumentTypes = {}, returnType = AccessType.MEMO)
    public static String dateS() {
        return LocalDate.now(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern("MM-dd-yyyy"));
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

    /**
     * Returns the second date if the first one is {@code null}.
     */
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
     * Returns the time of day of a string, on the base date 1899-12-30 like in Access. The string may contain a date,
     * which is ignored.
     *
     * @param value time like "13:14:15" or "1:14 PM", or a date with time
     * @return time on the base date, {@code null} if value is {@code null}
     */
    @FunctionType(functionName = "TimeValue", argumentTypes = {AccessType.MEMO}, returnType = AccessType.DATETIME)
    public static Timestamp timeValue(String value) {
        if (value == null) {
            return null;
        }
        RegionalSettings reg = getRegionalSettings();
        String s = value.trim().replace(reg.getAM(), "AM").replace(reg.getPM(), "PM");
        Matcher m = TIME_VALUE.matcher(s);
        if (isTime(m)) {
            int hour = Integer.parseInt(m.group(1));
            int minute = m.group(2) == null ? 0 : Integer.parseInt(m.group(2));
            int second = m.group(3) == null ? 0 : Integer.parseInt(m.group(3));
            String amPm = m.group(4);
            // like Access, AM and PM only apply to hours up to 12
            if (amPm != null && hour <= 12) {
                hour = hour % 12 + (Character.toUpperCase(amPm.charAt(0)) == 'P' ? 12 : 0);
            }
            if (hour > 23 || minute > 59 || second > 59) {
                UcanaccessRuntimeException.throwNow(String.format("Type mismatch in function TimeValue: %s", value));
            }
            return Timestamp.valueOf(ACCESS_BASE_DATE.withHour(hour).withMinute(minute).withSecond(second));
        }
        Timestamp dt = dateValue(value, false);
        if (dt == null) {
            UcanaccessRuntimeException.throwNow(String.format("Type mismatch in function TimeValue: %s", value));
        }
        return timeValue(dt);
    }

    /**
     * Returns whether a matcher of {@link #TIME_VALUE} matches a time, which needs minutes or AM/PM after the hour.
     */
    private static boolean isTime(Matcher m) {
        return m.matches() && (m.group(2) != null || m.group(4) != null);
    }

    /**
     * Returns the time of day of a date, on the base date 1899-12-30 like in Access.
     */
    @FunctionType(functionName = "TimeValue", argumentTypes = {AccessType.DATETIME}, returnType = AccessType.DATETIME)
    public static Timestamp timeValue(Timestamp value) {
        return value == null ? null : Timestamp.valueOf(ACCESS_BASE_DATE.with(value.toLocalDateTime().toLocalTime()));
    }

    /**
     * Returns the current system time as text in the format hh:mm:ss, like Time$ in Access.
     */
    @FunctionType(functionName = "\"TIME$\"", argumentTypes = {}, returnType = AccessType.MEMO)
    public static String timeS() {
        return LocalTime.now(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern("HH:mm:ss"));
    }

    /**
     * Returns the number of seconds since midnight, with fractions of a second.
     */
    @FunctionType(functionName = "Timer", argumentTypes = {}, returnType = AccessType.DOUBLE)
    public static double timer() {
        return LocalTime.now(ZoneId.systemDefault()).toNanoOfDay() / 1_000_000_000d;
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
     * Returns the octal representation of a number, rounded to a whole number first. Like in Access, negative numbers
     * in the range of a long integer are shown as 32-bit two's complement.
     *
     * @param number number to convert, if {@code null}, {@code null} is returned
     * @return octal digits
     */
    @FunctionType(functionName = "Oct", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.MEMO)
    public static String oct(Double number) {
        if (number == null) {
            return null;
        }
        long l = (long) Math.rint(number);
        return l == (int) l ? Integer.toOctalString((int) l) : Long.toOctalString(l);
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
    public static Double round(Double d) {
        return round(d, 0.0);
    }

    /**
     * Returns a number rounded to a specified number of decimal places. Like Access, halves are rounded to the nearest
     * even digit.
     */
    @FunctionType(namingConflict = true, functionName = "Round", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static Double round(Double d, Double p) {
        if (d == null || p == null) {
            return null;
        }
        // like Access, the number of decimal places is rounded as well and must not be negative
        int decimals = (int) Math.rint(p);
        if (decimals < 0) {
            UcanaccessRuntimeException.throwNow(String.format("Invalid number of decimal places in function Round: %s", p));
        }
        if (!Double.isFinite(d)) {
            return d;
        }
        return BigDecimal.valueOf(d).setScale(decimals, RoundingMode.HALF_EVEN).doubleValue();
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

    /**
     * Returns the range of the given size, like "11: 20", into which a number falls.
     *
     * @param number   number to find the range for, if {@code null}, {@code null} is returned
     * @param start    start of the overall range, at least 0
     * @param stop     end of the overall range, greater than start
     * @param interval size of each range, at least 1
     * @return range of the number, padded to the width of the largest value
     * @throws UcanaccessSQLException if start, stop or interval are invalid
     */
    @FunctionType(functionName = "PARTITION", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.MEMO)
    public static String partition(Double number, double start, double stop, double interval) throws UcanaccessSQLException {
        if (number == null) {
            return null;
        } else if (start < 0 || stop <= start || Math.rint(interval) < 1) {
            // like Access, and an interval below 1 would never end the loop below
            throw new InvalidFunctionParameterException("Partition", String.format("%s, %s, %s", start, stop, interval));
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
