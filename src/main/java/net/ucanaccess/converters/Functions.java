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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.TextStyle;
import java.time.temporal.ChronoUnit;
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
            return Boolean.parseBoolean((String) obj);
        } else {
            return obj instanceof Number && ((Number) obj).intValue() != 0;
        }
    }

    /**
     * Converts an expression into a currency value.
     */
    @FunctionType(functionName = "CCur", argumentTypes = {AccessType.CURRENCY}, returnType = AccessType.CURRENCY)
    public static BigDecimal ccur(BigDecimal value) {
        return value.setScale(4, RoundingMode.HALF_UP);
    }

    /**
     * Converts an expression into a date value.
     */
    @FunctionType(functionName = "CDate", argumentTypes = {AccessType.MEMO}, returnType = AccessType.DATETIME)
    public static Timestamp cdate(String dt) {
        return dateValue(dt, false);
    }

    /**
     * Converts an expression to a double.
     */
    @FunctionType(functionName = "CDbl", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static Double cdbl(Double number) {
        return number;
    }

    @FunctionType(functionName = "CDec", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static Double cdec(Double number) {
        return number;
    }

    @FunctionType(functionName = "CInt", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.INTEGER)
    public static Short cint(Double number) {
        return new BigDecimal((long) Math.floor(number + 0.499999999999999d)).shortValueExact();
    }

    @FunctionType(functionName = "CInt", argumentTypes = {AccessType.YESNO}, returnType = AccessType.INTEGER)
    public static Short cint(boolean bool) {
        return (short) (bool ? -1 : 0);
    }

    /**
     * Converts an expression to a long integer.
     */
    @FunctionType(functionName = "CLng", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.LONG)
    public static Integer clng(Double value) {
        return (int) Math.floor(value + 0.5d);
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

    @FunctionType(functionName = "CStr", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.MEMO)
    public static String cstr(double value) throws UcanaccessSQLException {
        return cstrImpl(value);
    }

    @FunctionType(functionName = "CStr", argumentTypes = {AccessType.LONG}, returnType = AccessType.MEMO)
    public static String cstr(int value) throws UcanaccessSQLException {
        return cstrImpl(value);
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
        Calendar cl = Calendar.getInstance();
        cl.setTime(dt);
        if ("yyyy".equalsIgnoreCase(intv)) {
            cl.add(Calendar.YEAR, vl);
        } else if ("q".equalsIgnoreCase(intv)) { // quarter
            cl.add(Calendar.MONTH, vl * 3);
        } else if ("m".equalsIgnoreCase(intv)) { // month
            cl.add(Calendar.MONTH, vl);
        } else if ("y".equalsIgnoreCase(intv)) { // day of year
            cl.add(Calendar.DAY_OF_YEAR, vl);
        } else if ("d".equalsIgnoreCase(intv)) { // day
            cl.add(Calendar.DAY_OF_YEAR, vl);
        } else if ("w".equalsIgnoreCase(intv)) { // weekday
            cl.add(Calendar.DAY_OF_WEEK, vl);
        } else if ("ww".equalsIgnoreCase(intv)) { // week
            cl.add(Calendar.WEEK_OF_YEAR, vl);
        } else if ("h".equalsIgnoreCase(intv)) { // hour
            cl.add(Calendar.HOUR, vl);
        } else if ("n".equalsIgnoreCase(intv)) { // minute
            cl.add(Calendar.MINUTE, vl);
        } else if ("s".equalsIgnoreCase(intv)) { // second
            cl.add(Calendar.SECOND, vl);
        } else {
            throw new InvalidIntervalValueException(intv);
        }
        return dt instanceof Timestamp
            ? new Timestamp(cl.getTimeInMillis())
            : new java.sql.Date(cl.getTimeInMillis());
    }

    @FunctionType(namingConflict = true, functionName = "DateAdd", argumentTypes = {AccessType.MEMO, AccessType.LONG, AccessType.DATETIME}, returnType = AccessType.DATETIME)
    public static Timestamp dateAdd(String intv, int vl, Timestamp dt) throws UcanaccessSQLException {
        return (Timestamp) dateAdd(intv, vl, (Date) dt);
    }

    @FunctionType(namingConflict = true, functionName = "DateAdd", argumentTypes = {AccessType.MEMO, AccessType.LONG, AccessType.MEMO}, returnType = AccessType.DATETIME)
    public static Timestamp dateAdd(String intv, int vl, String dt) throws UcanaccessSQLException {
        return (Timestamp) dateAdd(intv, vl, (Date) dateValue(dt, false));
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
        if (dt1 == null || intv == null || dt2 == null) {
            return null;
        }
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
        if ("yyyy".equalsIgnoreCase(intv)) {
            result = clMax.get(Calendar.YEAR) - clMin.get(Calendar.YEAR);
        } else if ("q".equalsIgnoreCase(intv)) {
            result = dateDiff("yyyy", dt1, dt2) * 4 + (clMax.get(Calendar.MONTH) - clMin.get(Calendar.MONTH)) / 3;
        } else if ("y".equalsIgnoreCase(intv) || "d".equalsIgnoreCase(intv)) {
            result = (int) Math.rint((double) (clMax.getTimeInMillis() - clMin.getTimeInMillis()) / (1000 * 60 * 60 * 24));
        } else if ("m".equalsIgnoreCase(intv)) {
            result = dateDiff("yyyy", dt1, dt2) * 12 + clMax.get(Calendar.MONTH) - clMin.get(Calendar.MONTH);
        } else if ("w".equalsIgnoreCase(intv) || "ww".equalsIgnoreCase(intv)) {
            result = (int) Math.floor((double) (clMax.getTimeInMillis() - clMin.getTimeInMillis()) / (1000 * 60 * 60 * 24 * 7));
        } else if ("h".equalsIgnoreCase(intv)) {
            result = (int) Math.round((clMax.getTime().getTime() - clMin.getTime().getTime()) / (1000d * 60 * 60));
        } else if ("n".equalsIgnoreCase(intv)) {
            result = (int) Math.rint((double) (clMax.getTimeInMillis() - clMin.getTimeInMillis()) / (1000 * 60));
        } else if ("s".equalsIgnoreCase(intv)) {
            result = (int) Math.rint((double) (clMax.getTimeInMillis() - clMin.getTimeInMillis()) / 1000);
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
            Calendar cl = Calendar.getInstance();
            cl.setTime(date);
            ret = cl.get(Calendar.DAY_OF_WEEK) - firstDayOfWeek + 1;
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
            Calendar cl = Calendar.getInstance();
            cl.setTime(dt);
            cl.set(Calendar.MONTH, Calendar.JANUARY);
            cl.set(Calendar.DAY_OF_MONTH, 1);
            Calendar cl1 = Calendar.getInstance();
            cl1.setTime(dt);
            if (firstDayOfWeek == 0) {
                firstDayOfWeek = 1;
            }
            int dow = cl.get(Calendar.DAY_OF_WEEK) - firstDayOfWeek + 1;
            if (dow <= 0) {
                dow = 7 + dow;
                if (cl1.get(Calendar.DAY_OF_WEEK) - firstDayOfWeek >= 0) {
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
        Calendar cl = Calendar.getInstance(Locale.US);
        cl.setTime(dt);
        if ("yyyy".equalsIgnoreCase(intv)) {
            return cl.get(Calendar.YEAR);
        } else if ("q".equalsIgnoreCase(intv)) {
            return (int) Math.ceil((cl.get(Calendar.MONTH) + 1) / 3d);
        } else if ("d".equalsIgnoreCase(intv)) {
            return cl.get(Calendar.DAY_OF_MONTH);
        } else if ("y".equalsIgnoreCase(intv)) {
            return cl.get(Calendar.DAY_OF_YEAR);
        } else if ("m".equalsIgnoreCase(intv)) {
            return cl.get(Calendar.MONTH) + 1;
        } else if ("ww".equalsIgnoreCase(intv)) {
            return cl.get(Calendar.WEEK_OF_YEAR);
        } else if ("w".equalsIgnoreCase(intv)) {
            return cl.get(Calendar.DAY_OF_WEEK);
        } else if ("h".equalsIgnoreCase(intv)) {
            return cl.get(Calendar.HOUR_OF_DAY);
        } else if ("n".equalsIgnoreCase(intv)) {
            return cl.get(Calendar.MINUTE);
        } else if ("s".equalsIgnoreCase(intv)) {
            return cl.get(Calendar.SECOND);
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
        Calendar cl = Calendar.getInstance();
        cl.setTime(dt);
        cl.set(Calendar.HOUR_OF_DAY, 0);
        cl.set(Calendar.MINUTE, 0);
        cl.set(Calendar.SECOND, 0);
        cl.set(Calendar.MILLISECOND, 0);
        return new Timestamp(cl.getTime().getTime());
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
        if (isNumeric(s)) {
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
        start--;
        if (compare != 0) {
            text = text.toLowerCase();
        }
        if (text.length() <= start) {
            return 0;
        } else {
            text = text.substring(start);
        }
        return text.indexOf(search) + start + 1;
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
        if (compare != 0) {
            text = text.toLowerCase();
        }
        if (text.length() <= start) {
            return 0;
        } else {
            if (start > 0) {
                text = text.substring(0, start);
            }
            return text.lastIndexOf(search) + 1;
        }
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
     * Extracts a substring from a string (starting at any position).
     */
    @FunctionType(functionName = "Mid", argumentTypes = {AccessType.MEMO, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String mid(String value, int start) {
        return mid(value, start, value.length());
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
        return Timestamp.valueOf(LocalDate.now().atStartOfDay());
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

    private static Object nz(Object value, Object other) {
        return value == null ? other : value;
    }

    /**
     * Returns the sign of a number. If number &gt; 0, it returns 1. If number = 0, it returns 0.
     */
    @FunctionType(functionName = "Sgn", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.INTEGER)
    public static short sgn(double n) {
        return (short) (n == 0 ? 0 : n > 0 ? 1 : -1);
    }

    @FunctionType(functionName = "Sign", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.INTEGER)
    public static short sign(double n) {
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
     * Returns a string representation of a number.
     */
    @FunctionType(functionName = "Str", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.TEXT)
    public static String str(double d) {
        String pre = d > 0 ? " " : "";
        return Math.round(d) == d ? pre + Math.round(d) : pre + d;
    }

    /**
     * Returns the current system time.
     */
    @FunctionType(functionName = "Time", argumentTypes = {}, returnType = AccessType.DATETIME)
    public static Timestamp time() {
        return new Timestamp(LocalDateTime.now()
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
        String val = val1.toString().trim();
        int lp = val.lastIndexOf('.');
        char[] ca = val.toCharArray();
        StringBuilder sb = new StringBuilder();
        int minLength = 1;
        for (int i = 0; i < ca.length; i++) {
            char c = ca[i];
            if ((c == '-' || c == '+') && i == 0) {
                minLength++;
                sb.append(c);
            } else if (c == ' ') {
                continue;
            } else if (Character.isDigit(c)) {
                sb.append(c);
            } else if (c == '.' && i == lp) {
                sb.append(c);
                if (i == 0 || i == 1 && minLength == 2) {
                    minLength++;
                }
            } else {
                break;
            }
        }
        if (sb.length() < minLength) {
            return 0.0d;
        } else {
            return Double.parseDouble(sb.toString());
        }
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
        int fdow = Math.min(Math.max(1, firstDayOfWeek), 7);
        int offset = fdow == 1 ? -1 : fdow - 2;
        int number = num;
        while (number > 7) {
            number -= 7;
        }
        return DayOfWeek.of(number).plus(offset)
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
     * Returns a time given an hour, minute, and second value.
     */
    @FunctionType(functionName = "TimeSerial", argumentTypes = {AccessType.LONG, AccessType.LONG, AccessType.LONG}, returnType = AccessType.DATETIME)
    public static Timestamp timeSerial(Integer h, Integer m, Integer s) {
        return new Timestamp(LocalDateTime.now()
            .withYear(1899).withMonth(12).withDayOfMonth(30)
            .truncatedTo(ChronoUnit.SECONDS)
            .withHour(h).withMinute(m).withSecond(s)
            .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
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
     * Returns a string converted as specified.
     */
    @FunctionType(functionName = "StrConv", argumentTypes = {AccessType.MEMO, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String strConv(String value, int conversion) {
        if (value == null) {
            return null;
        } else if (conversion == 1) { // vbUpperCase
            return value.toUpperCase();
        } else if (conversion == 2) { // vbLowerCase
            return value.toLowerCase();
        } else if (conversion == 3) { // vbProperCase: not implemented
            return value;
        }
        return value;
    }

    /**
     * Returns an integer value representing the result of a string comparison.
     */
    @FunctionType(functionName = "StrComp", argumentTypes = {AccessType.MEMO, AccessType.MEMO, AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer strComp(String value1, String value2, Integer type) throws UcanaccessSQLException {
        switch (type) {
            case 0:
            case -1:
            case 2:
                return value1.compareTo(value2);
            case 1:
                return value1.toUpperCase().compareTo(value2.toUpperCase());
            default:
                throw new InvalidFunctionParameterException("StrComp", type);
        }
    }

    @FunctionType(functionName = "StrComp", argumentTypes = {AccessType.MEMO, AccessType.MEMO}, returnType = AccessType.LONG)
    public static Integer strComp(String value1, String value2) throws UcanaccessSQLException {
        return strComp(value1, value2, 0);
    }

    /**
     * Returns the integer portion of a number.
     */
    @FunctionType(functionName = "Int", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.LONG)
    public static Integer mint(Double value) {
        return new BigDecimal((long) Math.floor(value)).intValueExact();
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
        if (cost < 0 || life == 2d && period > 1d) {
            return 0;
        }
        if (life < 2d || life == 2d && period <= 1d) {
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
        return pmt(rate, periods, pv, 0, 0);
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

        double y = 0;
        double y0 = 0;
        double y1 = 0;
        double x0 = 0;
        double f = 0;
        double i = 0;
        double rate = guess;
        if (Math.abs(rate) < financialPrecision) {
            y = pv * (1 + nper * rate) + pmt * (1 + rate * type) * nper + fv;
        } else {
            f = Math.exp(nper * Math.log(1 + rate));
            y = pv * f + pmt * (1 / rate + type) * (f - 1) + fv;
        }
        y0 = pv + pmt * nper + fv;
        y1 = pv * f + pmt * (1 / rate + type) * (f - 1) + fv;

        // find root by Newton secant method
        i = 0.0;
        x0 = 0.0;
        double x1 = rate;
        while (Math.abs(y0 - y1) > financialPrecision && i < financialMaxIterations) {
            rate = (y1 * x0 - y0 * x1) / (y1 - y0);
            x0 = x1;
            x1 = rate;

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
        Calendar clbb = Calendar.getInstance();
        clbb.set(1899, 11, 30, 0, 0, 0);
        return (double) dateDiff("y", new Timestamp(clbb.getTimeInMillis()), res);
    }

    @FunctionType(functionName = "formulaToBoolean", argumentTypes = {AccessType.YESNO, AccessType.MEMO}, returnType = AccessType.YESNO)
    public static Boolean formulaToBoolean(Boolean res, String datatype) {
        return res;
    }

    @FunctionType(functionName = "formulaToBoolean", argumentTypes = {AccessType.DOUBLE, AccessType.MEMO}, returnType = AccessType.YESNO)
    public static Boolean formulaToBoolean(Double res, String datatype) {
        return res == null ? null : res != 0d;
    }

    @FunctionType(functionName = "formulaToBoolean", argumentTypes = {AccessType.DATETIME, AccessType.MEMO}, returnType = AccessType.YESNO)
    public static Boolean formulaToBoolean(Timestamp res, String datatype) {
        return null;
    }

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
        Calendar cl = Calendar.getInstance();
        cl.setTimeInMillis(res.getTime());
        if (cl.get(Calendar.HOUR) == 0 && cl.get(Calendar.MINUTE) == 0 && cl.get(Calendar.SECOND) == 0) {
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
        Calendar clbb = Calendar.getInstance();
        clbb.set(1899, 11, 30, 0, 0, 0);
        clbb.set(Calendar.MILLISECOND, 0);
        return dateAdd("y", res ? -1 : 0, new Timestamp(clbb.getTimeInMillis()));
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
        Calendar clbb = Calendar.getInstance();
        clbb.set(1899, 11, 30, 0, 0, 0);
        clbb.set(Calendar.MILLISECOND, 0);
        Double d = Math.floor(res);
        Timestamp tr = dateAdd("y", d.intValue(), new Timestamp(clbb.getTimeInMillis()));
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
     * Returns a number rounded to a specified number of decimal places.
     */
    @FunctionType(namingConflict = true, functionName = "Round", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double round(double d, double p) {
        double f = Math.pow(10d, p);
        return Math.round(d * f) / f;
    }

    /**
     * Returns the integer portion of a number.
     *
     * @param number a double or any valid numeric expression, if {@code null}, {@code null} is returned
     * @return integer portion
     */
    @SuppressWarnings("PMD.UnnecessaryCast")
    @FunctionType(namingConflict = true, functionName = "Fix", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static Double fix(Double number) {
        return number == null
            ? null
            : sign(number) * (double) mint(Math.abs(number));
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
            return padLeft(-1, h) + ":" + padLeft(lrint(start) - 1, h);
        }
        if (number > stop) {
            return ul + ":" + padLeft(-1, h);
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

    private static String padLeft(int ext, int n) {
        String tp = ext > 0 ? String.valueOf(ext) : "";
        return String.format("%1$" + n + "s", tp);
    }

}
